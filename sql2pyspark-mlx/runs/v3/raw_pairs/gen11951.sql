WITH order_items_by_product AS (
    SELECT product_id, COUNT(*) order_item_count, SUM(unit_price * quantity) revenue
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.supplier_id,
    p.launch_date,
    oibp.order_item_count,
    oibp.revenue
FROM products p
INNER JOIN order_items_by_product oibp ON p.id = oibp.product_id
