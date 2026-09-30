SELECT
    status,
    customer_id,
    QUARTER(end_date) AS end_quarter,
    SUM(CASE WHEN plan = 'basic' THEN 1 ELSE 0 END) AS basic_count
FROM subscriptions
GROUP BY status, customer_id, QUARTER(end_date)
ORDER BY status ASC;
