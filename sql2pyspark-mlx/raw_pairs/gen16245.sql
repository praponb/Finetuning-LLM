WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        SUBSTRING(status, 1, 3) AS status_prefix
    FROM payments
    WHERE order_id IS NOT NULL
      AND method IS DISTINCT FROM 'paypal'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY paid_at DESC NULLS FIRST, id) = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM deduped
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        amount,
        paid_at,
        status,
        SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    status,
    island_id
FROM sessionized
ORDER BY status DESC NULLS LAST
