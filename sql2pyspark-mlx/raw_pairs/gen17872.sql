SELECT c.segment, ROUND(STDDEV(p.amount), 2) AS stddev_amount
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
INNER JOIN customers c ON o.customer_id = c.id
WHERE p.amount BETWEEN 1310 AND 1540
GROUP BY c.segment
ORDER BY 2 DESC, c.segment;
