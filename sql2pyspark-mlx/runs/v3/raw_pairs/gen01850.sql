SELECT a.id, a.store_id, a.units, a.product_id FROM store_sales a WHERE a.units = (SELECT MIN(b.units) FROM store_sales b WHERE b.store_id = a.store_id);
