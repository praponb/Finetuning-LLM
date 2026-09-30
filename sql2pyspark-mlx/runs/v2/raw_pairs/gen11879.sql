SELECT p.id, p.cost, p.launch_date, t.last_sale_date FROM products p INNER JOIN (SELECT product_id, MAX(sale_date) AS last_sale_date FROM store_sales GROUP BY product_id) t ON p.id = t.product_id;
