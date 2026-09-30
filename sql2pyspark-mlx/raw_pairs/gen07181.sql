WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    id,
    method,
    amount,
    paid_at,
    status,
    burst_id
FROM sessionized
ORDER BY status DESC NULLS LAST, paid_at DESC;
