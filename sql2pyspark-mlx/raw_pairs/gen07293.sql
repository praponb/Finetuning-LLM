WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        CAST(amount AS INT) AS amount_int
    FROM payments
    WHERE method = 'paypal'
      AND order_id IS NOT NULL
)
-- Running and moving aggregates
SELECT
    id,
    method,
    RANK() OVER (PARTITION BY method ORDER BY amount NULLS FIRST) AS amount_rank,
    CUME_DIST() OVER (ORDER BY amount_int DESC NULLS LAST) AS amount_int_cume_dist
FROM base
