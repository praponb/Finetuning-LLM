SELECT id, supplier_id, COALESCE(price, 0) AS price_filled FROM products WHERE UPPER(TRIM(product_name)) = 'YOGA MAT' ORDER BY id;
