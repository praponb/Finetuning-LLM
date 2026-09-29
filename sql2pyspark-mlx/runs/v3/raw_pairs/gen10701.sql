SELECT store_id, product_id, SUM(revenue) AS total_revenue FROM store_sales GROUP BY store_id, product_id;
