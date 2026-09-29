SELECT product_id, MIN(sale_date) AS min_sale_date FROM store_sales WHERE sale_date <= '2025-07-29' AND revenue < 1000 GROUP BY product_id;
