SELECT *, CONCAT_WS('-', txn_type, merchant) AS txn_type_merchant_key
FROM transactions
WHERE merchant IS NOT NULL AND merchant IN ('Starbucks', 'Lazada')
ORDER BY account_id DESC, id
LIMIT 5;
