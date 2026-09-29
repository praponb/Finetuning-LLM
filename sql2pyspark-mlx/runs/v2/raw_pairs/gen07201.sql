SELECT DISTINCT merchant
FROM transactions
WHERE TO_DATE(txn_time) >= '2023-09-09'
