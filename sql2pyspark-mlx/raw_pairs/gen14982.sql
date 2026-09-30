SELECT unit_price, 'order_item' kind
FROM order_items
UNION ALL
SELECT amount unit_price, 'payment' kind
FROM payments;
