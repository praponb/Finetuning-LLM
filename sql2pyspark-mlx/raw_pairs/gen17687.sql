SELECT id, store_id FROM store_sales INTERSECT SELECT id, store_id FROM store_sales WHERE revenue IS NULL
