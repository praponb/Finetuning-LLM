WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        item.product_id,
        item.qty,
        item.price
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS item
        FROM carts
    ) t
)
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    product_id,
    AVG(qty) AS avg_qty,
    MIN(created_at) AS first_seen,
    MAX(product_id) AS max_product_id
FROM flattened
GROUP BY DATE_TRUNC('DAY', created_at), DATE_TRUNC('HOUR', created_at), product_id;
