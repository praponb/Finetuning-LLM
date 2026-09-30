SELECT sale_date, store_id, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date, CAST(units AS DOUBLE) AS units_str FROM store_sales
