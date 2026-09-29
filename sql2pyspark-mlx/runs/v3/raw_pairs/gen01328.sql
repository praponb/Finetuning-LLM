SELECT id, amount, paid_at
FROM payments
WHERE method != 'bank_transfer'
