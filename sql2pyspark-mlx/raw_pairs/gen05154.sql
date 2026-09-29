SELECT city FROM customers INTERSECT SELECT brand FROM products WHERE UPPER(TRIM(product_name)) = 'BACKPACK XL';
