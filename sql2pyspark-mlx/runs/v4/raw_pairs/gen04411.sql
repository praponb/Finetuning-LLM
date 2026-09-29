SELECT a.id, a.product_id, a.units, a.store_id FROM store_sales a WHERE a.units < (SELECT AVG(b.units) FROM store_sales b WHERE b.product_id = a.product_id);
