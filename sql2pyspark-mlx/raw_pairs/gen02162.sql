SELECT MONTH(o.order_date) AS order_month, AVG(p.amount) AS avg_amount, COUNT(DISTINCT p.amount) AS unique_orders
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
GROUP BY MONTH(o.order_date)
