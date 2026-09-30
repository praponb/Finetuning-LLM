WITH product_stats AS (
    SELECT product_id, SUM(unit_price) AS total_unit_price, COUNT(DISTINCT quantity) AS unique_orders
    FROM order_items
    GROUP BY product_id
)
SELECT
    products.id,
    products.cost,
    products.stock_qty,
    product_stats.total_unit_price,
    product_stats.unique_orders
FROM products
INNER JOIN product_stats ON products.id = product_stats.product_id
