SELECT
    c.country,
    c.city,
    COUNT(*) AS cnt,
    SUM(a.amount) AS total_amount,
    MAX(a.discount) AS max_discount,
    GROUPING_ID() AS level_id
FROM orders a
INNER JOIN customers c ON a.customer_id = c.id
GROUP BY GROUPING SETS ((c.country, c.city), (c.country), ())
