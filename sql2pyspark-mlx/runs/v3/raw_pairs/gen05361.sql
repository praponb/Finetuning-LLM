SELECT product_id, COUNT(*) row_count, MAX(quantity) - MIN(quantity) quantity_range FROM inventory GROUP BY product_id ORDER BY product_id DESC LIMIT 3
