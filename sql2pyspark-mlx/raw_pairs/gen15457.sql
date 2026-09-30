SELECT a.id, a.product_id, a.quantity FROM inventory a WHERE a.quantity < (SELECT AVG(b.quantity) FROM inventory b WHERE b.product_id = a.product_id) ORDER BY a.product_id DESC
