SELECT txn_type, UPPER(merchant) AS merchant_upper, CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE txn_type <> 'REFUND'
ORDER BY txn_type, merchant_upper;
