SELECT brand
FROM products
WHERE brand = 'Initech'
INTERSECT
SELECT brand
FROM products
WHERE category IN ('Fashion', 'Sports', 'Grocery');
