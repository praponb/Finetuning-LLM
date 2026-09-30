SELECT *, CASE WHEN price >= 960 THEN 'High' WHEN price >= 590 THEN 'Medium' ELSE 'Low' END price_level, UPPER(category) category_upper
FROM products
WHERE price < 770
