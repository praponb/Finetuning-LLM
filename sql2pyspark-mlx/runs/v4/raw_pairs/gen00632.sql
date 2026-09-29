SELECT
    products.id,
    products.launch_date,
    products.cost,
    t.total_revenue,
    t.first_sale_date
FROM products
LEFT JOIN (SELECT product_id, SUM(revenue) AS total_revenue, MIN(sale_date) AS first_sale_date
    FROM store_sales
    GROUP BY product_id) t ON products.id = t.product_id
