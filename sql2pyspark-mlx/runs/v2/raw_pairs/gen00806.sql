SELECT id, product_id, store_id, COALESCE(revenue, 0) AS revenue_filled FROM store_sales;
