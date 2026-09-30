SELECT p.brand, MIN(ss.sale_date) AS first_sale_date FROM store_sales ss RIGHT JOIN products p ON p.id = ss.product_id WHERE p.supplier_id IN (11, 13) GROUP BY p.brand;
