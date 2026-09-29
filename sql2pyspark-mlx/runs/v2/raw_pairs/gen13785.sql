SELECT
    orders.id,
    orders.store_id,
    orders.channel,
    orders.order_date
FROM orders
WHERE orders.customer_id IS NOT NULL AND EXISTS (SELECT 1 FROM payments WHERE payments.order_id = orders.id AND payments.amount IS NOT NULL)
