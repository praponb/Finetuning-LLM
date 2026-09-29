WITH filtered AS (
    SELECT
        *,
        LOWER(method) AS method_lower,
        COALESCE(method, 'NONE') AS method_clean
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-08-24 00:00:00')
      AND status IN ('SUCCESS', 'REFUNDED')
),
deduped AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        method_lower
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY method ORDER BY amount DESC NULLS FIRST) = 1
),
flagged AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM deduped
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
metrics AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        method_lower,
        session_start_flag,
        LEAD(session_start_flag) OVER (PARTITION BY status, method ORDER BY island_id, id) AS next_session_start_flag,
        LAG(session_start_flag) OVER (PARTITION BY status, method ORDER BY island_id, id) AS prev_session_start_flag,
        LEAD(amount) OVER (PARTITION BY status, method ORDER BY island_id, id) AS next_amount,
        LAG(amount) OVER (PARTITION BY status, method ORDER BY island_id, id) AS prev_amount,
        ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY island_id, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY MONTH(paid_at), method ORDER BY session_start_flag, id) AS event_seq
    FROM islands
),
enriched AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY event_seq) AS event_seq_cume_dist,
        NTILE(10) OVER (PARTITION BY status, method ORDER BY next_session_start_flag, id) AS next_session_start_flag_bucket,
        SUM(next_amount) OVER (PARTITION BY status ORDER BY session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_amount,
        NTILE(5) OVER (PARTITION BY status ORDER BY next_amount DESC, id) AS next_amount_bucket
    FROM metrics
)
SELECT
    id,
    method,
    amount,
    status,
    method_lower,
    session_start_flag,
    next_session_start_flag,
    prev_session_start_flag,
    next_amount
FROM enriched
ORDER BY id DESC NULLS LAST
