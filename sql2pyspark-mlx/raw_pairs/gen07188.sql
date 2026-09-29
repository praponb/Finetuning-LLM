SELECT id, txn_type, CONCAT(txn_type, ' ', merchant) AS txn_type_merchant
FROM transactions
WHERE txn_time < '2023-10-18';
