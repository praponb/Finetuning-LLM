SELECT id, opened_date, city FROM stores WHERE opened_date = (SELECT MAX(opened_date) FROM stores WHERE sqft >= 9883);
