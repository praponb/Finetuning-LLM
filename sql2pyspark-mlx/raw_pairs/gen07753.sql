SELECT CASE WHEN amount >= 3100 THEN 'high' WHEN amount >= 1900 THEN 'mid' ELSE 'low' END AS amount_bucket, SUM(amount) AS total_amount
FROM transactions
WHERE merchant IN ('Shell', 'Lazada', 'Starbucks') OR amount IS NOT NULL
GROUP BY 1
