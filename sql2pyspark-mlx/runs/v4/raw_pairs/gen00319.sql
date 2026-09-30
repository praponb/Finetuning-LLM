SELECT s.id, s.store_id, s.units FROM store_sales s WHERE s.units = (SELECT MAX(s2.units) FROM store_sales s2 WHERE s2.store_id = s.store_id)
