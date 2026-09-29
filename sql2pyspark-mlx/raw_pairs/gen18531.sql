SELECT method
FROM payments
INTERSECT ALL
SELECT method
FROM payments
WHERE method IN ('paypal', 'cash', 'card');
