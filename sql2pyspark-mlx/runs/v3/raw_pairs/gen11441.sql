SELECT id, discount FROM orders WHERE discount = (SELECT MIN(discount) FROM orders WHERE store_id IS NOT NULL) ORDER BY id DESC, discount
