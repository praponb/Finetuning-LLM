SELECT *, UPPER(txn_type) AS txn_type_upper
FROM transactions
WHERE account_id IN (24, 33, 37) AND txn_type = 'FEE' AND merchant = 'Lazada'
