SELECT customer_id, COALESCE(channel, 'unassigned') AS channel_filled
FROM orders
WHERE status IN ('PENDING', 'SHIPPED', 'RETURNED') AND amount < 960 AND order_date BETWEEN '2025-03-20' AND '2025-10-01';
