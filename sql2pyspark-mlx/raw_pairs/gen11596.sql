SELECT id, price, brand, TRIM(brand) AS clean_brand FROM products ORDER BY clean_brand
