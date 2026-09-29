SELECT store_id, MIN(sale_date) AS min_sale_date FROM store_sales WHERE sale_date BETWEEN '2024-05-01' AND '2025-09-01' GROUP BY 1;
