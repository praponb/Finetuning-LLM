SELECT id, paid_at, amount, LAG(amount, 1) OVER (ORDER BY paid_at, id) AS prev_amount FROM payments WHERE TO_DATE(paid_at) >= '2023-05-01' ORDER BY amount DESC, prev_amount DESC;
