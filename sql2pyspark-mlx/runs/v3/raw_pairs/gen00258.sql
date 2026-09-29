SELECT o.customer_id, SUM(CASE WHEN s.status = 'IN_TRANSIT' THEN s.cost ELSE 0 END) AS in_transit_cost FROM shipments s JOIN orders o ON s.order_id = o.id GROUP BY o.customer_id;
