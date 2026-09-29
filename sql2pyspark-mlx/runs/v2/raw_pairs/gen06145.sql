SELECT DATE_FORMAT(o.order_date, 'yyyy-MM') order_month, ROUND(AVG(p.amount), 2) avg_amount, MAX(p.amount) max_amount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM')
HAVING ROUND(AVG(p.amount), 2) >= 75;
