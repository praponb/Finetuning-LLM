SELECT a.id, a.store_id, a.revenue FROM store_sales a WHERE a.revenue = (SELECT MAX(b.revenue) FROM store_sales b WHERE b.store_id = a.store_id)
