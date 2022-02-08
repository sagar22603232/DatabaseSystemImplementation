#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile()
{
    diskFilePtr = new File();
    currReadPtr = new Record();
    readPagePtr = new Page();
    writePagePtr = new Page();
}

DBFile::~DBFile()
{
    delete diskFilePtr;
    delete currReadPtr;
    delete readPagePtr;
    delete writePagePtr;
}

int DBFile::Create(const char *f_path, fType f_type, void *startup)
{
    switch (f_type)
    {
    case heap:
    {
        cout<<"Enter inside heap";
        diskFilePtr->Open(0, (char *)f_path);
        writePageValue = 0;
        readPageValue = 0;
        dirtyBitFlag = false;
        checkEnd = true;
        MoveFirst();
          break;
    }
  
    case sorted:
        // code block
        break;
    default:
        // code block
        break;
    }
    return 1;
}

void DBFile::Load(Schema &f_schema, const char *loadpath)
{
    FILE *fullFile = fopen(loadpath,"r");
    Record bufferRecord;
    while(bufferRecord.SuckNextRecord(&f_schema,fullFile)){
        Add(bufferRecord);
    }
    if(dirtyBitFlag){
        diskFilePtr->AddPage(writePagePtr,writePageValue);
    }
    fclose(fullFile);
}

int DBFile::Open(const char *f_path)
{
    diskFilePtr->Open(1, (char *)f_path);
    checkEnd = false;
    writePageValue =0;
    dirtyBitFlag = false;
    MoveFirst();
    return 1;
}

void DBFile::MoveFirst()
{
    int size = diskFilePtr->GetLength();
    if(size>0){
        readPagePtr->EmptyItOut();
        diskFilePtr->GetPage(this->readPagePtr,0);
        readPagePtr->GetFirst(currReadPtr);
    }
}

int DBFile::Close()
{
    if(dirtyBitFlag){
        diskFilePtr->AddPage(writePagePtr,writePageValue);
        writePageValue++;
    }
    cout<<"The file length"<<diskFilePtr->Close();
    return 1;
}

void DBFile::Add(Record &rec)
{
    dirtyBitFlag = true;
    Record bufferWriteRecord;
    bufferWriteRecord.Consume(&rec);
    int result = writePagePtr->Append(&bufferWriteRecord);
    if( result == 0){
        diskFilePtr->AddPage(writePagePtr, writePageValue);
        writePageValue++;
        writePagePtr->EmptyItOut();
        writePagePtr->Append(&bufferWriteRecord);
    }
}

int DBFile::GetNext(Record &fetchme)
{
    if(checkEnd!=true){
        fetchme.Copy(currReadPtr);
        int result = readPagePtr->GetFirst(currReadPtr);
        if(result == 0){
            readPageValue++;
            int size = diskFilePtr->GetLength()-1;
            if(readPageValue < size){
                 diskFilePtr->GetPage(readPagePtr, readPageValue);
                readPagePtr->GetFirst(currReadPtr);
            }
            else{   
                checkEnd = true;
               
            }
        }
        return 1;
    }
    return 0;
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    ComparisonEngine compare;
    while (GetNext(fetchme) == 1)
    {
       if(compare.Compare(&fetchme,&literal,&cnf) == 1){
           return 1;
       }
    }
    return 0;
    
}
