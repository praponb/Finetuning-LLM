SELECT amount value, 'payment' source
FROM payments
UNION
SELECT unit_price value, 'order_item' source
FROM order_items
WHERE unit_price BETWEEN 110 AND 390
