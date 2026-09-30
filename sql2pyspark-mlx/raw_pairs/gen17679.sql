WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(paid_at AS DATE) AS paid_at_day,
        COALESCE(method, 'N/A') AS method_filled
    FROM payments
    WHERE status = 'SUCCESS'
)
-- Join a dimension and aggregate
SELECT
    dim.channel,
    src.status AS payment_status,
    COUNT(*) AS rows_total,
    MAX(src.amount) AS max_amount
FROM base src
INNER JOIN orders dim ON src.order_id = dim.id
GROUP BY GROUPING SETS ((dim.channel, src.status), (dim.channel), ())
ORDER BY channel DESC NULLS LAST;
