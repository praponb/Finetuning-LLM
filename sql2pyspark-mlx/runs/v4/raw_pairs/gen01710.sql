SELECT o.shipping_fee, o.store_id, p.paid_at, p.amount, p.status FROM orders o INNER JOIN payments p ON p.order_id = o.id WHERE o.channel <> 'partner' ORDER BY p.status DESC
