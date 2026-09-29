SELECT amount, 'transaction' source
FROM transactions
UNION
SELECT price, 'product' source
FROM products
WHERE product_name IS NOT NULL
ORDER BY amount DESC;
