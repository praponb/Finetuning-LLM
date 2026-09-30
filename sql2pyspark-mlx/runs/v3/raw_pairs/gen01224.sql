SELECT id, store_id FROM store_sales WHERE revenue < 3100 INTERSECT SELECT id, store_id FROM store_sales WHERE sale_date < '2023-09-15';
