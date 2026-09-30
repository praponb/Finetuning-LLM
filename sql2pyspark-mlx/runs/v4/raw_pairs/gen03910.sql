SELECT
    is_active,
    SUM(CASE WHEN city = 'Singapore' THEN age ELSE 0 END) singapore_age,
    ROUND(AVG(age), 2) avg_age,
    SUM(age) total_age
FROM customers
WHERE country IN ('TH', 'DE', 'US')
GROUP BY is_active;
