SELECT
    CASE WHEN amount >= 3200 THEN 'high' WHEN amount >= 2000 THEN 'mid' ELSE 'low' END AS amount_bucket,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    SUM(SUM(amount)) OVER (ORDER BY CASE WHEN amount >= 3200 THEN 'high' WHEN amount >= 2000 THEN 'mid' ELSE 'low' END) AS cumulative_amount
FROM transactions
GROUP BY CASE WHEN amount >= 3200 THEN 'high' WHEN amount >= 2000 THEN 'mid' ELSE 'low' END
ORDER BY cumulative_amount DESC;
