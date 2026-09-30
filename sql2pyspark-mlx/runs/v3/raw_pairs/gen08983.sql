SELECT id, region, opened_date, sqft, LAG(sqft, 1) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft FROM stores WHERE store_name LIKE 'D%' ORDER BY id DESC
