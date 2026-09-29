SELECT store_id, product_id, revenue, CAST(units AS DOUBLE) AS units_num, revenue - units AS revenue_per_unit FROM store_sales WHERE sale_date <= '2023-12-01' AND units >= 27 ORDER BY units_num DESC
