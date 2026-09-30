SELECT txn_type, CONCAT(merchant, ', ', txn_type) AS merchant_txn_type
FROM transactions
WHERE account_id = 16 AND merchant IN ('Shell', 'Lazada') AND account_id IN (21, 32, 35, 40)
