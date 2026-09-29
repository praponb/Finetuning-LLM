SELECT
    id,
    order_id,
    method,
    amount,
    status,
    FIRST_VALUE(method) IGNORE NULLS OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
    DENSE_RANK() OVER w_part AS amount_dense_rank
FROM payments
WHERE amount >= 530
WINDOW w_part AS (PARTITION BY order_id ORDER BY amount DESC NULLS LAST);
