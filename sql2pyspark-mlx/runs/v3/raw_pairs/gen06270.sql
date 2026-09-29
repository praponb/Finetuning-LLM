SELECT QUARTER(o.order_date) AS order_quarter, ROUND(AVG(p.amount), 2) AS avg_amount, MAX(p.amount) AS max_amount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
GROUP BY QUARTER(o.order_date)
