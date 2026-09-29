SELECT o.discount, s.delivered_date, s.order_id, SUBSTRING(s.carrier, 1, 4) AS carrier_prefix FROM orders o INNER JOIN shipments s ON o.id = s.order_id;
