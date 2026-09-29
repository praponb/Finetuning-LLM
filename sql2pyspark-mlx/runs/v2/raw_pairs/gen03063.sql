SELECT
    p.id,
    p.category,
    p.product_name,
    t.unique_products
FROM products p
INNER JOIN (SELECT product_id, COUNT(DISTINCT quantity) AS unique_products
    FROM order_items
    GROUP BY product_id) t ON p.id = t.product_id;
