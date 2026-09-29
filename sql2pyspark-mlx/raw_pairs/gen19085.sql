SELECT id, units, product_id FROM store_sales WHERE sale_date BETWEEN '2025-07-20' AND '2025-09-01' INTERSECT SELECT id, units, product_id FROM store_sales WHERE store_id = 3;
