SELECT paid_at, method, status
FROM payments
WHERE NOT (status NOT IN ('FAILED', 'REFUNDED') AND order_id IN (22, 24))
ORDER BY 2;
