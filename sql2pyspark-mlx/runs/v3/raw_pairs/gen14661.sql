SELECT CASE WHEN amount >= 1430 THEN 'high' WHEN amount >= 460 THEN 'mid' ELSE 'low' END AS amount_bucket, COUNT(*) AS cnt
FROM payments
GROUP BY CASE WHEN amount >= 1430 THEN 'high' WHEN amount >= 460 THEN 'mid' ELSE 'low' END
ORDER BY amount_bucket;
