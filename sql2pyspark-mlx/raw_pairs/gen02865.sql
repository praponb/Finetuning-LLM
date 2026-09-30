SELECT id, price FROM products WHERE price >= (SELECT MAX(price) FROM products WHERE ARRAY_CONTAINS(tags, 'clearance')) ORDER BY price DESC;
