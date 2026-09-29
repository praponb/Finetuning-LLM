SELECT
    DATE_TRUNC('MONTH', opened_date) opened_month,
    customer_id,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values,
    COUNT(balance) balance_count,
    COUNT(CASE WHEN status = 'OPEN' THEN 1 END) open_count
FROM accounts
WHERE balance < 76000 AND status = 'CLOSED' AND balance <= 46000
GROUP BY CUBE(DATE_TRUNC('MONTH', opened_date), customer_id)
ORDER BY open_count
