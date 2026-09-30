SELECT id, store_id FROM store_sales WHERE revenue <= 1600 EXCEPT SELECT id, store_id FROM store_sales WHERE revenue IS NOT NULL
