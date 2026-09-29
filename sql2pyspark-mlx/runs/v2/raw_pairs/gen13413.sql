SELECT store_id, sale_date, CEIL(revenue) AS revenue_whole FROM store_sales WHERE store_id IN (1, 2, 3, 4) AND revenue < 1100 ORDER BY store_id DESC;
