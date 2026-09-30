WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), paid_at) / 30 AS months_since_paid_at,
        COALESCE(method, 'DIRECT') AS method_filled
    FROM payments
    WHERE TO_DATE(paid_at) < '2024-10-03'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        paid_at,
        months_since_paid_at,
        SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY island_id, id) AS dedup_rank
        FROM sessionized
    ) t
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.*,
        d.discount,
        d.channel,
        d.order_date
    FROM deduped s
    INNER JOIN orders d
        ON s.order_id = d.id
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM joined
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Ranking and gap features
SELECT
    id,
    order_id,
    paid_at,
    island_id,
    order_date,
    session_start_flag,
    session_id,
    FIRST_VALUE(session_start_flag, TRUE) OVER (PARTITION BY order_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_session_start_flag,
    SUM(session_id) OVER (PARTITION BY DATE_TRUNC('YEAR', paid_at), order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS session_id_sum_90d,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY session_start_flag, id) AS seq_no
FROM sessions_payments
