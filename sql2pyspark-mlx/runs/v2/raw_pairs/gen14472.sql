SELECT
    orders.store_id,
    orders.shipping_fee,
    payments.method,
    payments.status,
    payments.paid_at,
    CONCAT_WS('-', payments.method, payments.status) AS method_status_key
FROM orders
LEFT JOIN payments ON payments.order_id = orders.id
WHERE orders.status IN ('COMPLETED', 'SHIPPED') AND payments.method = 'card'
ORDER BY paid_at;
