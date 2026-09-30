WITH product_totals AS (
    SELECT product_id, COUNT(DISTINCT unit_price) AS unique_products, MIN(quantity) AS min_quantity
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.category,
    p.price,
    pt.unique_products,
    pt.min_quantity
FROM products p
LEFT OUTER JOIN product_totals pt ON p.id = pt.product_id
WHERE pt.min_quantity > 7.7;
