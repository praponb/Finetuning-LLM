SELECT
    account_type,
    DATE_TRUNC('MONTH', opened_date) AS opened_month,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values,
    SUM(balance) AS total_balance
FROM accounts
WHERE status IS NULL
GROUP BY account_type, opened_month
ORDER BY opened_month;
