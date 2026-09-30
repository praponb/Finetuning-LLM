WITH order_stats AS (
    SELECT order_id, MIN(unit_price) AS min_unit_price
    FROM order_items
    WHERE unit_price > 310
    GROUP BY order_id
)
SELECT o.id, o.store_id, os.min_unit_price
FROM orders o
LEFT OUTER JOIN order_stats os ON o.id = os.order_id
WHERE os.min_unit_price >= 170
ORDER BY store_id DESC
