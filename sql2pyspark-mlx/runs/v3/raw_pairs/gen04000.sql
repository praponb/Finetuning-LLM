SELECT MONTH(orders.order_date) AS order_month, orders.customer_id, MAX(payments.paid_at) AS last_paid_at
FROM payments
LEFT OUTER JOIN orders ON payments.order_id = orders.id
LEFT OUTER JOIN customers ON customers.id = orders.customer_id
WHERE payments.status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED')
GROUP BY MONTH(orders.order_date), orders.customer_id
ORDER BY last_paid_at;
