SELECT p.id, p.brand, COUNT(ss.id) AS store_sale_count
FROM products p
LEFT OUTER JOIN store_sales ss ON p.id = ss.product_id
WHERE ss.sale_date < '2023-10-01'
GROUP BY p.id, p.brand;
