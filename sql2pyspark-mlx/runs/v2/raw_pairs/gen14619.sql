SELECT c.credit_limit, s.device
FROM customers c
INNER JOIN sessions s ON c.id = s.customer_id
WHERE c.country IN ('DE', 'TH');
