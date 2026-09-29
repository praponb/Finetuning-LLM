SELECT *, UPPER(product_name) product_name_upper, CASE WHEN brand = 'Stark' THEN 1 ELSE 0 END is_stark FROM products WHERE LOWER(TRIM(product_name)) = 'coffee maker' AND supplier_id IS NOT NULL;
