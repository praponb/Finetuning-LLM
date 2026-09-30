SELECT
    CASE WHEN age >= 58 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    CASE WHEN credit_limit >= 15200 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
    SUM(CASE WHEN city = 'New York' THEN credit_limit ELSE 0 END) AS new_york_credit_limit,
    SUM(CASE WHEN city = 'Chiang Mai' THEN age ELSE 0 END) AS chiang_mai_age,
    AVG(credit_limit) FILTER (WHERE city = 'London') AS london_avg_credit_limit,
    MAX(credit_limit) AS max_credit_limit,
    GROUPING(CASE WHEN age >= 58 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_id
FROM customers
GROUP BY GROUPING SETS ((CASE WHEN age >= 58 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN credit_limit >= 15200 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN age >= 58 THEN 'PREMIUM' ELSE 'STANDARD' END));
