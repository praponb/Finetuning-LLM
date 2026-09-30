SELECT
    MONTH(txn_time) txn_month,
    account_id,
    SUM(CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END) fee_count,
    SORT_ARRAY(COLLECT_SET(merchant)) merchant_list,
    COUNT(amount) amount_count
FROM transactions
GROUP BY MONTH(txn_time), account_id
ORDER BY account_id DESC, fee_count DESC, txn_month
LIMIT 5
