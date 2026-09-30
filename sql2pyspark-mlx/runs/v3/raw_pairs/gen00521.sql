SELECT
    o.amount,
    o.channel,
    o.order_date,
    p.paid_at,
    p.status,
    p.order_id
FROM orders o
JOIN payments p ON o.id = p.order_id
WHERE p.method NOT IN ('bank_transfer', 'promptpay', 'paypal');
