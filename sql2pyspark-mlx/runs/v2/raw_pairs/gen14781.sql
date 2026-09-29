SELECT *, INITCAP(TRIM(txn_type)) AS txn_type_title
FROM transactions
WHERE is_flagged AND amount >= 3500 AND TO_DATE(txn_time) >= '2023-09-01'
