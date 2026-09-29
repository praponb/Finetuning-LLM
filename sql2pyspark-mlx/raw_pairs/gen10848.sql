WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        AVG(duration_sec) OVER (ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_3_prior,
        LAG(duration_sec, 2) OVER (PARTITION BY YEAR(event_time), event_type ORDER BY duration_sec, id) AS prev_duration_sec
    FROM web_events
    WHERE event_type = 'purchase'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM metrics
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_time,
        duration_sec_avg_3_prior,
        SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY session_id_2, id) AS seq_no,
        LAST_VALUE(duration_sec_avg_3_prior, TRUE) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec_avg_3_prior
    FROM sessions_web_events
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_time,
        duration_sec_avg_3_prior,
        session_id_2,
        seq_no,
        last_known_duration_sec_avg_3_prior,
        PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY session_id_2) AS session_id_2_percent_rank,
        FIRST_VALUE(seq_no, TRUE) OVER (PARTITION BY session_id ORDER BY session_id_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no,
        AVG(duration_sec_avg_3_prior) OVER (ORDER BY duration_sec_avg_3_prior, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_3_prior_avg_6_prior
    FROM enriched
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_time,
        duration_sec_avg_3_prior,
        session_id_2,
        seq_no,
        session_id_2_percent_rank,
        first_known_seq_no,
        COALESCE(duration_sec_avg_3_prior_avg_6_prior, 0) AS duration_sec_avg_3_prior_avg_6_prior_filled,
        ROUND(duration_sec_avg_3_prior_avg_6_prior * 1.15, 2) AS duration_sec_avg_3_prior_avg_6_prior_adjusted
    FROM with_windows
    WHERE event_time > TIMESTAMP '2023-03-28 09:00:00'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (ORDER BY session_id_2, id) AS session_id_2_bucket,
        DENSE_RANK() OVER (ORDER BY duration_sec_avg_3_prior DESC) AS duration_sec_avg_3_prior_dense_rank,
        LAG(duration_sec_avg_3_prior_avg_6_prior_adjusted, 2) OVER (PARTITION BY MONTH(event_time) ORDER BY session_id_2_percent_rank, id) AS prev_duration_sec_avg_3_prior_avg_6_prior_adjusted,
        AVG(seq_no) OVER w_running AS seq_no_avg_6_prior
    FROM filtered
    WINDOW w_running AS (PARTITION BY DATE_TRUNC('month', event_time) ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
metrics_web_events AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(duration_sec_avg_3_prior_dense_rank, 2, 0) OVER (ORDER BY session_id_2, id) AS prev_duration_sec_avg_3_prior_dense_rank,
        MAX(prev_duration_sec_avg_3_prior_avg_6_prior_adjusted) OVER (PARTITION BY DATE_TRUNC('WEEK', event_time)) AS event_time_period_max_prev_duration_sec_avg_3_prior_avg_6_prior_adjusted,
        AVG(session_id_2) OVER (ORDER BY seq_no, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS session_id_2_avg_2_prior,
        LAST_VALUE(first_known_seq_no) IGNORE NULLS OVER (ORDER BY duration_sec_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_seq_no,
        LAG(session_id_2, 1) OVER (ORDER BY prev_duration_sec_avg_3_prior_avg_6_prior_adjusted, id) AS prev_session_id_2
    FROM scored
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    first_known_seq_no,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    COUNT(*) AS row_count,
    MAX(event_time) AS last_event_time,
    MIN(event_time) AS first_event_time,
    AVG(prev_session_id_2) AS avg_prev_session_id_2,
    SUM(seq_no) AS total_seq_no
FROM metrics_web_events
GROUP BY ROLLUP (DATE_TRUNC('MONTH', event_time), first_known_seq_no, DATE_TRUNC('QUARTER', event_time))
ORDER BY quarter_start DESC;
