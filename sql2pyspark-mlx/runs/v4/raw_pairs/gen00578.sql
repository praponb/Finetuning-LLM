SELECT id, region, opened_date, LOWER(store_name) store_name_lower, COALESCE(store_name, 'N/A') store_name_filled FROM stores ORDER BY region
