WITH order_items_by_order AS (
    SELECT order_id, AVG(quantity) AS avg_quantity
    FROM order_items
    WHERE product_id IS NULL
    GROUP BY order_id
)
SELECT
    o.id,
    o.shipping_fee,
    o.store_id,
    oibo.avg_quantity
FROM orders o
LEFT JOIN order_items_by_order oibo ON o.id = oibo.order_id
WHERE oibo.avg_quantity > 3.3;
