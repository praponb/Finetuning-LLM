WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        created_at,
        items,
        ship_to,
        AGGREGATE(items, 0D, (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS cart_value,
        attributes['device'] AS device
    FROM carts
    WHERE HOUR(created_at) BETWEEN 7 AND 12
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    MAX(created_at) AS ended_at,
    MAX(cart_value) AS max_cart_value,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    PERCENTILE_APPROX(cart_value, 0.95) AS cart_value_p95
FROM flattened_attrs
GROUP BY DATE_TRUNC('HOUR', created_at)
HAVING MAX(cart_value) >= 0;
