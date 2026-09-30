WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, value
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        created_at,
        key,
        value,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY created_at DESC, id, key) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY created_at, id, key) AS row_seq,
        FIRST_VALUE(key, TRUE) OVER (PARTITION BY key ORDER BY created_at, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_key
    FROM flattened
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(created_at) OVER (PARTITION BY key ORDER BY created_at, id, key) AS prev_created_at
    FROM scored
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_created_at IS NULL OR created_at > prev_created_at + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY key ORDER BY created_at, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        created_at,
        key,
        value,
        seq_no,
        row_seq,
        first_known_key,
        prev_created_at,
        session_start_flag,
        island_id,
        NTILE(4) OVER (PARTITION BY key ORDER BY island_id, id, key) AS island_id_bucket,
        RANK() OVER (PARTITION BY key ORDER BY island_id DESC) AS island_id_rank,
        MIN(session_start_flag) OVER (PARTITION BY key) AS key_min_session_start_flag
    FROM sessionized
)
SELECT
    id,
    created_at,
    key,
    row_seq,
    first_known_key,
    prev_created_at,
    island_id,
    island_id_bucket,
    island_id_rank,
    CASE WHEN key RLIKE '^(device|utm_source)$' THEN 'CORE' ELSE 'PRIORITY' END AS key_zone
FROM enriched;
