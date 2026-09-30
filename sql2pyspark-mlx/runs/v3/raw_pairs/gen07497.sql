WITH order_items_by_product AS (
    SELECT product_id, COUNT(quantity) AS quantity_count
    FROM order_items
    WHERE unit_price <= 380
    GROUP BY product_id
)
SELECT p.stock_qty, p.cost, oibp.quantity_count
FROM products p
INNER JOIN order_items_by_product oibp ON p.id = oibp.product_id
WHERE oibp.quantity_count > 8;
