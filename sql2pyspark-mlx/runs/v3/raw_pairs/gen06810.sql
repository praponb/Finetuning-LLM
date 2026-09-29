SELECT id, category, price, EXPLODE(tags) AS tag FROM products WHERE product_name LIKE 'N%';
