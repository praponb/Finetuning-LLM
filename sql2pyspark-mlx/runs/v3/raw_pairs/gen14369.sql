SELECT txn_type, amount, CONCAT(txn_type, ' ', merchant) AS txn_type_merchant
FROM transactions
WHERE amount IS NOT NULL OR TO_DATE(txn_time) >= '2024-10-01';
