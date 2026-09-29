SELECT a.id, a.store_id, a.units FROM store_sales a WHERE a.units > (SELECT AVG(b.units) FROM store_sales b WHERE b.store_id = a.store_id)
