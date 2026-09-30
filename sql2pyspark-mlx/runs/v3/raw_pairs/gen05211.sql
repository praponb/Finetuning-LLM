WITH order_items_by_product AS (
    SELECT product_id, COUNT(DISTINCT quantity) AS unique_products, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.cost,
    oibp.unique_products,
    oibp.stddev_quantity
FROM products p
LEFT JOIN order_items_by_product oibp ON p.id = oibp.product_id
ORDER BY cost DESC
