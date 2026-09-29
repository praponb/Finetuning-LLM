WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        HOUR(paid_at) paid_at_hour,
        INITCAP(TRIM(status)) status_title
    FROM payments
    WHERE method IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.method,
        src.amount,
        src.paid_at_hour,
        src.status_title,
        d.channel,
        d.amount AS order_amount
    FROM filtered src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY order_amount DESC, id) AS rn
    FROM enriched
    QUALIFY rn = 1
),
latest_payments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, order_id ORDER BY rn, id) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
)
-- Join a dimension and aggregate
SELECT
    dim.channel AS order_channel,
    a.channel AS payment_channel,
    COUNT(*) AS num_rows,
    ROUND(SUM(a.amount), 2) AS total_amount,
    GROUPING_ID() AS gid
FROM latest_payments a
INNER JOIN orders dim ON a.order_id = dim.id
GROUP BY GROUPING SETS ((dim.channel, a.channel), (dim.channel), ());
