SELECT amount
FROM orders
WHERE status <> 'CANCELLED'
UNION ALL
SELECT unit_price
FROM order_items
WHERE quantity > 4
ORDER BY amount;
