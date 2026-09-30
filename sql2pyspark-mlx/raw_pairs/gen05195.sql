SELECT discount
FROM orders
WHERE (channel IN ('store', 'mobile') AND channel = 'store') OR order_date < '2025-09-01'
