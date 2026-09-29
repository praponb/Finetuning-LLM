SELECT SORT_ARRAY(COLLECT_SET(txn_type)) txn_type_list, SUM(amount) total_amount, COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) fee_count
FROM transactions
WHERE is_flagged OR txn_type = 'REFUND'
ORDER BY fee_count DESC;
