WITH order_items_by_product AS (
    SELECT product_id, MIN(quantity) AS min_quantity, SUM(quantity * unit_price) AS revenue
    FROM order_items
    GROUP BY product_id
)
SELECT products.stock_qty, order_items_by_product.min_quantity, order_items_by_product.revenue
FROM products
JOIN order_items_by_product ON products.id = order_items_by_product.product_id
ORDER BY min_quantity DESC;
