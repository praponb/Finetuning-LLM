SELECT QUARTER(opened_date) opened_quarter, MAX(sqft) - MIN(sqft) sqft_range FROM stores WHERE LOWER(TRIM(store_name)) = 'siam flagship' GROUP BY QUARTER(opened_date)
