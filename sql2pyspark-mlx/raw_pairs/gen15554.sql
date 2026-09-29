SELECT s.order_id, s.status, s.shipped_date, o.order_date, ROUND(o.shipping_fee * 1.15, 2) AS shipping_fee_scaled FROM shipments s LEFT OUTER JOIN orders o ON s.order_id = o.id;
