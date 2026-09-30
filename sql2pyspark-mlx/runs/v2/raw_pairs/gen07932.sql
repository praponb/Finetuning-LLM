SELECT
    is_active,
    SUM(age) AS total_age,
    COUNT(*) AS cnt,
    ROUND(SUM(age) * 100.0 / SUM(SUM(age)) OVER (), 2) AS pct_of_total
FROM customers
GROUP BY is_active
