SELECT category
FROM products
EXCEPT
SELECT status
FROM orders
WHERE channel IN ('store', 'mobile')
ORDER BY category;
