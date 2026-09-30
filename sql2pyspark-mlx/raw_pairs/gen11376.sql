SELECT o.status, COUNT(p.id) AS payment_count, AVG(p.amount) AS avg_amount
FROM orders o
LEFT JOIN payments p ON o.id = p.order_id
WHERE YEAR(o.order_date) >= 2025
GROUP BY o.status
ORDER BY payment_count, avg_amount DESC, status
LIMIT 10;
