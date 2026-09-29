SELECT
    id,
    paid_at,
    order_id,
    amount,
    ROW_NUMBER() OVER (ORDER BY paid_at DESC, id) row_num,
    FIRST_VALUE(paid_at) OVER (PARTITION BY order_id ORDER BY amount DESC, id) first_paid_at
FROM payments
WHERE order_id IN (8, 13, 29, 34);
