SELECT
    dim.city,
    e.plan,
    COUNT(*) AS cnt,
    MAX(e.monthly_fee) AS max_monthly_fee,
    GROUPING_ID() AS gid
FROM subscriptions e
INNER JOIN customers dim ON e.customer_id = dim.id
GROUP BY ROLLUP (dim.city, e.plan);
