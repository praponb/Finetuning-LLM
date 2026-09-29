SELECT
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    SUM(balance) AS total_balance,
    MIN(opened_date) AS first_opened_date,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values
FROM accounts
GROUP BY opened_month
ORDER BY account_type_values DESC
