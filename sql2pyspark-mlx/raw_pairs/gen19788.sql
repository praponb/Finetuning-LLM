SELECT
    c.city,
    s.status,
    COUNT(*) AS cnt,
    ROUND(AVG(s.balance), 2) AS avg_balance,
    GROUPING_ID() AS level_id
FROM accounts s
JOIN customers c ON s.customer_id = c.id
GROUP BY GROUPING SETS ((c.city, s.status), (c.city), ())
ORDER BY avg_balance
