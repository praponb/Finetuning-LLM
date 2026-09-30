SELECT
    CASE WHEN amount >= 1530 THEN 'high' WHEN amount >= 440 THEN 'mid' ELSE 'low' END AS amount_bucket,
    status,
    COUNT(DISTINCT amount) AS unique_methods,
    ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
WHERE amount > 1380
GROUP BY CASE WHEN amount >= 1530 THEN 'high' WHEN amount >= 440 THEN 'mid' ELSE 'low' END, status
ORDER BY 3 DESC
