SELECT
    is_active,
    SUM(age) total_age,
    COUNT(*) cnt,
    SUM(SUM(age)) OVER (ORDER BY is_active) AS cumulative_age
FROM customers
GROUP BY is_active
ORDER BY cumulative_age DESC, total_age;
