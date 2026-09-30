SELECT id, product_id, sale_date, revenue, SUM(revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS running_revenue FROM store_sales
