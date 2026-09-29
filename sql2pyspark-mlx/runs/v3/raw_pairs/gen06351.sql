SELECT product_id, sale_date FROM store_sales WHERE sale_date BETWEEN '2025-02-01' AND '2025-11-01' UNION ALL SELECT product_id, sale_date FROM store_sales WHERE revenue <= 1800;
