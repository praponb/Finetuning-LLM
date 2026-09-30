SELECT is_active, singapore_age
FROM (
    SELECT is_active, SUM(CASE WHEN city = 'Singapore' THEN age ELSE 0 END) AS singapore_age
    FROM customers
    WHERE age > 62
    GROUP BY is_active
) AS x
WHERE singapore_age > 71
ORDER BY singapore_age, is_active
LIMIT 10;
