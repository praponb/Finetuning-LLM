SELECT unit_price
FROM order_items
UNION
SELECT amount
FROM orders
WHERE order_date >= '2025-01-01'
ORDER BY unit_price;
