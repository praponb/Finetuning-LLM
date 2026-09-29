WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(country) AS clean_country
    FROM sessions
    WHERE browser IS NOT NULL
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) AS prev_started_at
    FROM base
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        browser,
        started_at,
        country,
        clean_country,
        SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        started_at,
        clean_country,
        LEAD(session_id) OVER w_grp AS next_session_id,
        LAG(session_id) OVER w_grp AS prev_session_id,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM sessions_sessions
    WINDOW w_grp AS (PARTITION BY country ORDER BY started_at, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        clean_country,
        seq_no,
        SUM(seq_no) OVER (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS seq_no_sum_90d,
        MAX(prev_session_id) OVER (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS prev_session_id_max_30d,
        LAST_VALUE(seq_no) IGNORE NULLS OVER (ORDER BY prev_session_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no,
        ROW_NUMBER() OVER (ORDER BY next_session_id DESC, id) AS event_seq,
        LAG(seq_no, 1) OVER (ORDER BY next_session_id, id) AS prev_seq_no
    FROM enriched
    WHERE seq_no BETWEEN 5 AND 6
),
enriched_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        clean_country,
        seq_no,
        seq_no_sum_90d,
        prev_session_id_max_30d,
        LEAD(seq_no) OVER w_part AS next_seq_no,
        LAG(seq_no) OVER w_part AS prev_seq_no_2,
        LAG(last_known_seq_no) OVER w_part AS prev_last_known_seq_no,
        ROW_NUMBER() OVER w_part AS row_seq,
        SUM(prev_session_id_max_30d) OVER (ORDER BY last_known_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_session_id_max_30d
    FROM metrics
    WINDOW w_part AS (ORDER BY seq_no, id)
)
SELECT
    id,
    seq_no_sum_90d,
    prev_session_id_max_30d,
    next_seq_no,
    prev_last_known_seq_no,
    row_seq,
    cumulative_prev_session_id_max_30d
FROM enriched_sessions
ORDER BY prev_session_id_max_30d DESC;
