SELECT p.brand, COUNT(ss.id) store_sale_count, MIN(ss.sale_date) first_sale_date
FROM products p
LEFT JOIN store_sales ss ON p.id = ss.product_id
GROUP BY p.brand;
