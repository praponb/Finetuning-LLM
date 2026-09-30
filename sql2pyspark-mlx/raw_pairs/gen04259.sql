SELECT id, amount, ROW_NUMBER() OVER (ORDER BY amount DESC, id) rn
FROM payments
