SELECT
    id,
    order_id,
    paid_at,
    amount,
    SUM(amount) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS running_amount,
    RANK() OVER (PARTITION BY order_id ORDER BY amount) AS amount_rank
FROM payments;
