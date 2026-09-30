SELECT
    CASE WHEN amount >= 1570 THEN 'high' WHEN amount >= 550 THEN 'mid' ELSE 'low' END AS amount_bucket,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    SUM(SUM(amount)) OVER (ORDER BY CASE WHEN amount >= 1570 THEN 'high' WHEN amount >= 550 THEN 'mid' ELSE 'low' END) AS cumulative_amount
FROM payments
GROUP BY CASE WHEN amount >= 1570 THEN 'high' WHEN amount >= 550 THEN 'mid' ELSE 'low' END
