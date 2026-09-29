SELECT CASE WHEN amount >= 4000 THEN 'high' WHEN amount >= 1200 THEN 'mid' ELSE 'low' END AS amount_bucket, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY CASE WHEN amount >= 4000 THEN 'high' WHEN amount >= 1200 THEN 'mid' ELSE 'low' END) AS cumulative_amount
FROM transactions
GROUP BY CASE WHEN amount >= 4000 THEN 'high' WHEN amount >= 1200 THEN 'mid' ELSE 'low' END
ORDER BY amount_bucket;
