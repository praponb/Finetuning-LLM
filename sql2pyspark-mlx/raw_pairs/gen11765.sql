SELECT
    id,
    amount,
    method,
    paid_at,
    ROW_NUMBER() OVER (ORDER BY amount DESC, id) row_num,
    SUM(amount) OVER (PARTITION BY method ORDER BY paid_at, id) running_amount
FROM payments
WHERE paid_at >= '2023-04-01'
ORDER BY amount;
