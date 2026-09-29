SELECT o.discount, t.max_cost, t.in_transit_count
FROM orders o
LEFT OUTER JOIN (SELECT order_id, MAX(cost) max_cost, COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END) in_transit_count
    FROM shipments
    WHERE carrier = 'DHL'
    GROUP BY order_id) t ON o.id = t.order_id
ORDER BY discount DESC, t.in_transit_count;
