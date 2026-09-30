SELECT id, region, sqft, city, opened_date, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) sqft_row, LEAD(sqft, 1) OVER (PARTITION BY city ORDER BY opened_date, id) next_sqft FROM stores;
