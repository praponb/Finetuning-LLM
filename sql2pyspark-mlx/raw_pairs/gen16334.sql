SELECT *, DATE_FORMAT(paid_at, 'MMM yyyy') paid_at_label
FROM payments
WHERE amount <= 870 AND status = 'FAILED'
ORDER BY amount DESC, id
LIMIT 50;
