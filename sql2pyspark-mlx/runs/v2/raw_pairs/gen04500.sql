SELECT o.id, o.shipping_fee FROM orders o WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND s.carrier = 'DHL')
