SELECT amount
FROM transactions
UNION ALL
SELECT unit_price AS amount
FROM order_items
WHERE unit_price BETWEEN 220 AND 340
ORDER BY amount
