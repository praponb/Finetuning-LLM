WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        amount,
        status,
        CONCAT_WS('-', status, method) AS status_method_key
    FROM payments
    WHERE status <> 'SUCCESS'
      AND paid_at > TIMESTAMP '2023-10-14 18:00:00'
)
-- Join a dimension and aggregate
SELECT
    dim.channel,
    s.status AS payment_status,
    COUNT(*) AS rows_total,
    SUM(s.amount) AS total_amount,
    GROUPING_ID() AS grouping_level
FROM filtered s
JOIN orders dim ON s.order_id = dim.id
GROUP BY CUBE (dim.channel, s.status);
