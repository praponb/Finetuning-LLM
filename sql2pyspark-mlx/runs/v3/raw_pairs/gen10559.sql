SELECT o.shipping_fee, s.delivered_date, s.shipped_date
FROM orders o
LEFT OUTER JOIN shipments s ON o.id = s.order_id;
