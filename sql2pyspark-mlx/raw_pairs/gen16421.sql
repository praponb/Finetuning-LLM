SELECT amount
FROM transactions
EXCEPT
SELECT unit_price
FROM order_items
ORDER BY amount DESC
