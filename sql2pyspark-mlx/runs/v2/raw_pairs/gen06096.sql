SELECT store_id, MIN(revenue) AS min_revenue FROM store_sales GROUP BY store_id HAVING MIN(revenue) < 2000
