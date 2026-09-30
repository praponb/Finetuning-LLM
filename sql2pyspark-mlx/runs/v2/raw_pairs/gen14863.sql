SELECT o.status, COUNT(*) total_count FROM shipments s LEFT OUTER JOIN orders o ON s.order_id = o.id WHERE s.carrier <> 'FedEx' GROUP BY o.status ORDER BY total_count DESC, status DESC;
