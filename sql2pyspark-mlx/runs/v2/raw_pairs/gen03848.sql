SELECT id, product_id, units, revenue, FIRST_VALUE(revenue) OVER (PARTITION BY product_id ORDER BY units DESC, id) AS first_revenue FROM store_sales;
