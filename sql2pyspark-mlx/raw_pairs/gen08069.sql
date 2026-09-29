WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        duration_sec,
        LAG(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time, id) AS row_seq,
        MAX(duration_sec) OVER (PARTITION BY session_id) AS session_id_max_duration_sec
    FROM web_events
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, session_id_max_duration_sec + row_seq AS session_id_max_duration_sec_plus_row_seq
    FROM metrics
    WHERE session_id_max_duration_sec BETWEEN 198 AND 291
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        duration_sec,
        row_seq,
        session_id_max_duration_sec,
        session_id_max_duration_sec_plus_row_seq,
        LAG(duration_sec, 2, 0) OVER (ORDER BY row_seq, id) AS prev_duration_sec_2,
        LEAD(prev_duration_sec, 1) OVER (ORDER BY session_id_max_duration_sec, id) AS next_prev_duration_sec,
        SUM(session_id_max_duration_sec_plus_row_seq) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_session_id_max_duration_sec_plus_row_seq,
        SUM(row_seq) OVER (ORDER BY prev_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        LEAD(session_id_max_duration_sec, 2, 0) OVER (ORDER BY session_id_max_duration_sec_plus_row_seq, id) AS next_session_id_max_duration_sec
    FROM base
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY cumulative_session_id_max_duration_sec_plus_row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        AVG(duration_sec) OVER (ORDER BY session_id_max_duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_avg_6
    FROM with_windows
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN prev_duration_sec_2 >= 513 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_duration_sec_2_tier,
        CASE WHEN next_session_id_max_duration_sec >= 477 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_session_id_max_duration_sec_tier,
        COUNT(*) AS num_rows,
        ROUND(AVG(cumulative_session_id_max_duration_sec_plus_row_seq), 2) AS avg_cumulative_session_id_max_duration_sec_plus_row_seq,
        SUM(row_seq) AS total_row_seq,
        COUNT(DISTINCT id) AS distinct_ids,
        GROUPING_ID() AS grouping_level
    FROM enriched
    GROUP BY GROUPING SETS ((CASE WHEN prev_duration_sec_2 >= 513 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN next_session_id_max_duration_sec >= 477 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN prev_duration_sec_2 >= 513 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
)
SELECT
    next_session_id_max_duration_sec_tier,
    avg_cumulative_session_id_max_duration_sec_plus_row_seq,
    total_row_seq,
    distinct_ids,
    grouping_level
FROM totals
