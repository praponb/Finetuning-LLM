SELECT id, region, opened_date, sqft, LAG(sqft, 1) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft, ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS row_num FROM stores
