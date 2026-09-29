SELECT
    p.id,
    p.price,
    t.min_revenue,
    t.max_revenue
FROM products p
INNER JOIN (SELECT product_id, MIN(revenue) AS min_revenue, MAX(revenue) AS max_revenue
    FROM store_sales
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.max_revenue > 3300;
