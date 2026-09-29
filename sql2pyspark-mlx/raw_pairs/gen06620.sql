SELECT id, paid_at, status
FROM payments
WHERE method IN ('cash', 'paypal', 'bank_transfer') OR amount >= 770;
