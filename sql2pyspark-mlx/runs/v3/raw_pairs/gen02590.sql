SELECT id, opened_date, store_name FROM stores WHERE opened_date = (SELECT MAX(opened_date) FROM stores)
