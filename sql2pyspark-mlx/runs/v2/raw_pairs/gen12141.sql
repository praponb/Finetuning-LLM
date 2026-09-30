SELECT o.shipping_fee, s.delivered_date
FROM orders o
LEFT OUTER JOIN shipments s ON o.id = s.order_id
WHERE s.delivered_date <= '2025-10-11' OR o.discount >= 0.31
ORDER BY delivered_date ASC;
