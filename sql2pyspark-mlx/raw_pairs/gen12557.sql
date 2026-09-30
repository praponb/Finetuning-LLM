SELECT o.channel, ROUND(AVG(p.amount), 2) AS avg_amount FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id GROUP BY o.channel HAVING avg_amount >= 20
