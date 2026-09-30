SELECT
    dim.city,
    e.plan,
    COUNT(*) AS rows_total,
    AVG(e.monthly_fee) AS avg_monthly_fee
FROM subscriptions e
INNER JOIN customers dim ON e.customer_id = dim.id
GROUP BY CUBE (dim.city, e.plan)
