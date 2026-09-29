SELECT customer_id, MAX(order_date) AS last_order_date, SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END) AS partner_count
FROM orders
WHERE amount > 790 AND (order_date > '2025-03-18' OR customer_id IN (15, 21))
GROUP BY customer_id;
