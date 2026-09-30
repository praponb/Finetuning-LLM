WITH recent_order_items AS (
    SELECT *
    FROM order_items
    WHERE product_id IS NOT NULL AND unit_price <= 280 AND unit_price >= 100
),
order_items_stats AS (
    SELECT order_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range
    FROM recent_order_items
    GROUP BY order_id
)
SELECT order_id, unit_price_range
FROM order_items_stats;
