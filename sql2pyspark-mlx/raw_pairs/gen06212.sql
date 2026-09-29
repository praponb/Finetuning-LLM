SELECT *, ROUND(price * 0.9, 2) AS adjusted_price
FROM products
WHERE category IN ('Home', 'Books')
ORDER BY category DESC, brand;
