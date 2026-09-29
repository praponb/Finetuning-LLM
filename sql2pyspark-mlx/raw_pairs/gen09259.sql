SELECT id, city, opened_date, sqft, LAG(sqft, 1) OVER (PARTITION BY city ORDER BY opened_date, id) AS prev_sqft, ROW_NUMBER() OVER (ORDER BY sqft DESC, id) AS row_num FROM stores
