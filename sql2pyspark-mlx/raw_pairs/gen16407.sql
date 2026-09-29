SELECT id, discount, store_id FROM orders WHERE discount >= (SELECT MAX(discount) FROM orders) ORDER BY id DESC
