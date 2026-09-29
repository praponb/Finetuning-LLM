SELECT revenue
FROM store_sales
WHERE revenue IS NOT NULL
UNION
SELECT cost AS revenue
FROM products
WHERE cost IS NOT NULL;
