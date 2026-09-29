SELECT
    city,
    CASE WHEN age >= 58 THEN 'high' WHEN age >= 25 THEN 'mid' ELSE 'low' END AS age_bucket,
    COUNT(*) AS num_customers,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    MAX(age) - MIN(age) AS age_range
FROM customers
WHERE country = 'TH' AND segment <> 'Corporate'
GROUP BY city, CASE WHEN age >= 58 THEN 'high' WHEN age >= 25 THEN 'mid' ELSE 'low' END
ORDER BY age_range DESC;
