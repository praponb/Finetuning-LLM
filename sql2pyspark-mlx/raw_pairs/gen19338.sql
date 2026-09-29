SELECT id, store_name, city, sqft, LAST_DAY(opened_date) AS opened_date_month_end, COALESCE(sqft, 0) AS sqft_filled FROM stores WHERE store_name LIKE '%Town%'
