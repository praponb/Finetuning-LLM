SELECT amount, 'transaction' AS source
FROM transactions
UNION ALL
SELECT unit_price AS amount, 'order_item' AS source
FROM order_items
ORDER BY amount
