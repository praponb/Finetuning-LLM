SELECT id, price FROM products WHERE price >= (SELECT MAX(price) FROM products WHERE product_name LIKE '%Backp%') ORDER BY id DESC LIMIT 50
