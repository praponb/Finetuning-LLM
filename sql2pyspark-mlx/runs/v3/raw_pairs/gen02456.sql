SELECT
    CASE WHEN shipping_fee >= 40 THEN 'high' WHEN shipping_fee >= 20 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket,
    channel,
    COUNT(DISTINCT shipping_fee) AS unique_statuss,
    ROUND((100 * SUM(CASE WHEN channel = 'mobile' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_mobile
FROM orders
GROUP BY shipping_fee_bucket, channel
HAVING pct_mobile >= 45
