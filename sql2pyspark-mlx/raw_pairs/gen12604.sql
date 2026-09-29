SELECT
    status_group,
    status,
    last_order_date,
    min_shipping_fee
FROM (
    SELECT
        CASE WHEN status IN ('CANCELLED', 'COMPLETED') THEN 'tier_1' ELSE 'other' END AS status_group,
        status,
        MAX(order_date) AS last_order_date,
        MIN(shipping_fee) AS min_shipping_fee
    FROM orders
    GROUP BY CASE WHEN status IN ('CANCELLED', 'COMPLETED') THEN 'tier_1' ELSE 'other' END, status
) AS x
WHERE min_shipping_fee > 34
ORDER BY status_group DESC, status
LIMIT 10;
