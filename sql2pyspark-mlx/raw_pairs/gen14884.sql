SELECT o.store_id, o.order_date, o.shipping_fee, p.order_id, p.paid_at FROM orders o LEFT JOIN payments p ON p.order_id = o.id WHERE p.method IS NOT NULL AND p.status IN ('FAILED', 'REFUNDED')
