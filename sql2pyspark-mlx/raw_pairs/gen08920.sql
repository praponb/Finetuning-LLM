WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(page, TRUE) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page,
        ROW_NUMBER() OVER (ORDER BY event_time DESC, id) AS row_seq,
        STDDEV_SAMP(duration_sec) OVER (PARTITION BY session_id) AS session_id_stddev_samp_duration_sec,
        AVG(duration_sec) OVER w_ordered AS duration_sec_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY duration_sec, id) AS seq_no
    FROM web_events
    WINDOW w_ordered AS (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
metrics_web_events AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(seq_no) OVER w_grp AS running_count_seq_no,
        MIN(seq_no) OVER w_grp AS running_min_seq_no,
        SUM(seq_no) OVER w_grp AS running_sum_seq_no
    FROM metrics
    WINDOW w_grp AS (PARTITION BY page, session_id ORDER BY session_id_stddev_samp_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        b.browser,
        b.country,
        b.device
    FROM metrics_web_events e
    LEFT JOIN sessions b
        ON e.session_id = b.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_dims
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM sessionized
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type, page ORDER BY event_time DESC, id) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        event_type,
        page,
        duration_sec,
        first_known_page,
        session_id_stddev_samp_duration_sec,
        duration_sec_avg_3_prior,
        running_min_seq_no,
        is_new_burst,
        FIRST_VALUE(running_min_seq_no, TRUE) OVER (PARTITION BY event_type ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_min_seq_no,
        SUM(is_new_burst) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS is_new_burst_sum_30d
    FROM ranked
),
metrics_web_events_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        duration_sec,
        first_known_page,
        session_id_stddev_samp_duration_sec,
        duration_sec_avg_3_prior,
        is_new_burst,
        first_known_running_min_seq_no,
        is_new_burst_sum_30d,
        SUM(first_known_running_min_seq_no) OVER (PARTITION BY event_type) AS event_type_sum_first_known_running_min_seq_no,
        RANK() OVER w_running AS session_id_stddev_samp_duration_sec_rank
    FROM with_windows
    WINDOW w_running AS (PARTITION BY event_type ORDER BY session_id_stddev_samp_duration_sec DESC)
)
SELECT
    id,
    event_type,
    duration_sec,
    first_known_page,
    is_new_burst,
    session_id_stddev_samp_duration_sec_rank,
    CASE WHEN event_type RLIKE '^(search|purchase)$' THEN 'GROUP_A' WHEN event_type RLIKE '^(page_view|click)$' THEN 'CORE' ELSE 'EXTENDED' END AS event_type_zone
FROM metrics_web_events_2
ORDER BY duration_sec NULLS FIRST;
