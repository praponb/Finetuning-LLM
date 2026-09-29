SELECT id, last_restock, product_id FROM inventory WHERE last_restock = (SELECT MIN(last_restock) FROM inventory) ORDER BY id DESC, last_restock LIMIT 50
