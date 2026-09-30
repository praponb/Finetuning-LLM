SELECT sale_date FROM store_sales WHERE units IS NOT NULL INTERSECT SELECT sale_date FROM store_sales WHERE store_id IN (2, 3, 5)
