SELECT
    p.country,
    e.plan,
    COUNT(*) AS num_rows,
    SUM(e.monthly_fee) AS total_monthly_fee,
    GROUPING_ID() AS grouping_level
FROM subscriptions e
INNER JOIN customers p ON e.customer_id = p.id
GROUP BY GROUPING SETS ((p.country, e.plan), (p.country), ());
