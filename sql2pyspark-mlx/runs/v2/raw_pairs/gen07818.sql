WITH order_items_by_product AS (
    SELECT product_id, COUNT(DISTINCT quantity) AS unique_products
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.stock_qty,
    p.product_name,
    oibp.unique_products
FROM products p
LEFT OUTER JOIN order_items_by_product oibp ON p.id = oibp.product_id
ORDER BY product_name, id;
