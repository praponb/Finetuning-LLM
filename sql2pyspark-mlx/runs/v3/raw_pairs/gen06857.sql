SELECT c.city, SUM(p.amount) AS total_amount, ROUND(STDDEV(p.amount), 2) AS stddev_amount
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
LEFT JOIN customers c ON o.customer_id = c.id
WHERE o.discount >= 0.16
GROUP BY c.city
HAVING ROUND(STDDEV(p.amount), 2) >= 42
ORDER BY total_amount
