SELECT
    id,
    order_id,
    amount,
    RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) AS amount_rank
FROM payments
ORDER BY 4 DESC, order_id DESC
