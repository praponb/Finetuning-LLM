SELECT
    status,
    CASE WHEN channel IN ('web', 'store') THEN 'tier_1' ELSE 'other' END AS channel_group,
    MIN(shipping_fee) AS min_shipping_fee,
    SUM(CASE WHEN channel = 'store' THEN discount ELSE 0 END) AS store_discount,
    MAX(discount) - MIN(discount) AS discount_range
FROM orders
WHERE channel IN ('partner', 'web') AND channel = 'store'
GROUP BY CUBE(status, CASE WHEN channel IN ('web', 'store') THEN 'tier_1' ELSE 'other' END)
ORDER BY store_discount DESC;
