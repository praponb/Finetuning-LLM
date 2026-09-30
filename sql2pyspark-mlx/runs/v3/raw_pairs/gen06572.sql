SELECT store_sales.id, store_sales.store_id, store_sales.units FROM store_sales WHERE store_sales.product_id IN (SELECT id FROM products WHERE ARRAY_CONTAINS(tags, 'eco'));
