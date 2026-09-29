SELECT id, opened_date FROM stores WHERE opened_date = (SELECT MAX(opened_date) FROM stores) ORDER BY id DESC LIMIT 5
