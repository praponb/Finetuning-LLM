SELECT
    segment,
    is_active,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    SUM(CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END) AS consumer_count
FROM customers
GROUP BY segment, is_active
HAVING ROUND(AVG(credit_limit), 2) >= 66
ORDER BY avg_credit_limit DESC
