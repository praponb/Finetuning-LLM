SELECT id, paid_at
FROM payments
WHERE method IN ('cash', 'paypal') OR status = 'FAILED';
