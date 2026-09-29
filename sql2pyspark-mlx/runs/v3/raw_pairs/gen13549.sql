SELECT id, opened_date, sqft FROM stores WHERE store_name NOT LIKE 'Old%' ORDER BY id DESC LIMIT 5;
