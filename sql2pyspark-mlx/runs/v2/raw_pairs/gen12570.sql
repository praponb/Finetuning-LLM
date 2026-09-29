SELECT o.status, MIN(s.delivered_date) min_delivered_date FROM shipments s RIGHT JOIN orders o ON o.id = s.order_id WHERE s.cost <= 80 GROUP BY o.status ORDER BY min_delivered_date, status LIMIT 5;
