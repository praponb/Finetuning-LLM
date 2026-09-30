SELECT a.id, a.product_id, a.units FROM store_sales a WHERE a.units > (SELECT AVG(b.units) FROM store_sales b WHERE b.product_id = a.product_id) ORDER BY a.id DESC, a.units DESC;
