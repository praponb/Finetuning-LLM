SELECT id, region, city, opened_date, sqft, LAG(sqft, 1) OVER (PARTITION BY region, city ORDER BY opened_date, id) AS prev_sqft FROM stores WHERE sqft BETWEEN 4695 AND 13967
