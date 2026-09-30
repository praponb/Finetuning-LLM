SELECT id, product_name, COALESCE(brand, 'unassigned') AS brand_filled FROM products
