SELECT
    CASE WHEN shipping_fee >= 31 THEN 'high' WHEN shipping_fee >= 5 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket,
    status,
    SUM(CASE WHEN status = 'PENDING' THEN discount ELSE 0 END) AS pending_discount,
    COUNT(*) AS row_count
FROM orders
WHERE channel IN ('store', 'web') AND amount > 1000 AND store_id IS NULL
GROUP BY 1, 2
ORDER BY pending_discount DESC
