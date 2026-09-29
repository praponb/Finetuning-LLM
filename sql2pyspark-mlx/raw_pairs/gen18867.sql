SELECT *
FROM (
    SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, MAX(balance) - MIN(balance) AS balance_range, SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
    FROM accounts
    WHERE account_type IN ('SAVINGS', 'LOAN', 'CREDIT')
    GROUP BY DATE_TRUNC('MONTH', opened_date)
) AS agg
WHERE balance_range >= 55
ORDER BY opened_month DESC
