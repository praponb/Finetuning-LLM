SELECT
    CASE WHEN amount >= 1360 THEN 'high' WHEN amount >= 580 THEN 'mid' ELSE 'low' END amount_bucket,
    SUM(amount) total_amount,
    COUNT(*) cnt,
    ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total
FROM orders
GROUP BY CASE WHEN amount >= 1360 THEN 'high' WHEN amount >= 580 THEN 'mid' ELSE 'low' END
ORDER BY cnt DESC, amount_bucket
LIMIT 1;
