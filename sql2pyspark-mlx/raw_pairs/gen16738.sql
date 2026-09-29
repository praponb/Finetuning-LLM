SELECT
    d.segment,
    d.city,
    a.status,
    COUNT(*) AS num_rows,
    MIN(a.monthly_fee) AS min_monthly_fee,
    GROUPING_ID() AS grouping_level
FROM subscriptions a
INNER JOIN customers d ON a.customer_id = d.id
GROUP BY GROUPING SETS ((d.segment, d.city, a.status), (d.segment, d.city), (d.segment), ())
