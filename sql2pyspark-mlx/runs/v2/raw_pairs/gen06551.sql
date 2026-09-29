SELECT s.id, s.shipped_date, s.delivered_date, s.status FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE channel IN ('store', 'mobile', 'partner'));
