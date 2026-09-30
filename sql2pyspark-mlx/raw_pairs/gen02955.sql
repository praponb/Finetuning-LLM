SELECT price
FROM products
WHERE category != 'Sports'
UNION
SELECT amount
FROM transactions;
