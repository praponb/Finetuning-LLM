SELECT status
FROM orders
WHERE amount BETWEEN 550 AND 830
UNION
SELECT status
FROM payments;
