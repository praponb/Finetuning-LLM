SELECT amount, CONCAT_WS('/', merchant, txn_type) AS merchant_txn_type_key
FROM transactions
WHERE merchant IN ('Lazada', 'Shell', '7-Eleven') AND txn_type = 'DEBIT'
