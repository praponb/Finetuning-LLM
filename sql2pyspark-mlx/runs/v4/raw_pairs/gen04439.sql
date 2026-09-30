SELECT amount, is_flagged, INITCAP(TRIM(txn_type)) AS txn_type_title
FROM transactions
ORDER BY txn_type_title, amount DESC
