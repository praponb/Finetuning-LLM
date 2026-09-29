SELECT txn_type_group, amount_range, merchant_list
FROM (
    SELECT CASE WHEN txn_type IN ('CREDIT', 'DEBIT') THEN 'group_a' ELSE 'other' END AS txn_type_group, MAX(amount) - MIN(amount) AS amount_range, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
    FROM transactions
    GROUP BY CASE WHEN txn_type IN ('CREDIT', 'DEBIT') THEN 'group_a' ELSE 'other' END
) AS agg
WHERE amount_range < 43
ORDER BY amount_range DESC;
