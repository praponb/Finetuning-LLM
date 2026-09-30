SELECT id, paid_at
FROM payments
WHERE method IS NOT NULL AND (order_id IS NOT NULL OR amount BETWEEN 810 AND 1560)
ORDER BY paid_at DESC, id
LIMIT 3
