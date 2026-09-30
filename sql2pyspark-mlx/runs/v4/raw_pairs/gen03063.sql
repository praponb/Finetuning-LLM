SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    NTILE(5) OVER (PARTITION BY order_id, method ORDER BY amount DESC, id) AS amount_bucket,
    COUNT(amount) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_count_90d
FROM payments;
