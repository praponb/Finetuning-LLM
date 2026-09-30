WITH valid AS (
    SELECT
        *,
        CAST(amount AS INT) AS amount_int,
        TRIM(method) AS clean_method
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-05-05 14:00:00'
      AND method IS NOT NULL
),
base AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_int,
        COALESCE(method, 'Unknown') AS method_filled,
        COALESCE(method, 'N/A') AS resolved_method
    FROM valid
    WHERE paid_at > TIMESTAMP '2023-11-03 09:00:00'
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY paid_at NULLS LAST, id) AS dedup_rank
        FROM base
    ) x
    WHERE dedup_rank = 1
),
prev_events AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_paid_at
    FROM latest
),
gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY paid_at DESC NULLS FIRST, id) AS row_num
    FROM islands
    QUALIFY row_num = 1
)
SELECT
    id,
    amount,
    paid_at,
    status,
    method_filled,
    resolved_method,
    dedup_rank,
    prev_paid_at,
    session_no,
    CASE WHEN status RLIKE '^(FAILED|REFUNDED)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS status_zone
FROM deduped
