SELECT DATE_TRUNC('MONTH', o.order_date) AS order_month, SUM(p.amount) AS total_amount
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
WHERE MONTH(o.order_date) IN (1, 7, 11)
GROUP BY DATE_TRUNC('MONTH', o.order_date)
