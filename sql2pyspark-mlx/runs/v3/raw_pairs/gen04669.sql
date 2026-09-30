SELECT store_id, product_id, MIN(revenue) min_revenue FROM store_sales WHERE sale_date <= '2023-03-16' GROUP BY store_id, product_id HAVING MIN(revenue) >= 3500;
