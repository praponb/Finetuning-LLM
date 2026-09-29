SELECT
    account_type,
    COUNT(*) AS cnt,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    SUM(CASE WHEN account_type = 'SAVINGS' THEN 1 ELSE 0 END) AS savings_count
FROM accounts
WHERE status != 'OPEN'
GROUP BY 1;
