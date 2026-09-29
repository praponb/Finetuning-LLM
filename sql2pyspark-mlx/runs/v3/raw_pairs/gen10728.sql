SELECT o.id, o.shipping_fee, o.channel
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.method = 'bank_transfer')
ORDER BY channel;
