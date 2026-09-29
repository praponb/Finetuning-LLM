SELECT CASE WHEN shipping_fee >= 40 THEN 'high' WHEN shipping_fee >= 11 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket, COUNT(*) AS row_count
FROM orders
WHERE shipping_fee BETWEEN 21 AND 22 OR channel = 'mobile'
GROUP BY shipping_fee_bucket
ORDER BY shipping_fee_bucket DESC, row_count DESC;
