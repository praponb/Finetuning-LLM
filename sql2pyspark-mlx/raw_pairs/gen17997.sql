WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_count_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_duration_sec
    FROM web_events
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_event_time
    FROM with_windows
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        duration_sec,
        last7_count_duration_sec,
        last7_avg_duration_sec,
        last7_sum_duration_sec,
        RANK() OVER w_grp session_no_rank,
        ROW_NUMBER() OVER (PARTITION BY page, session_id ORDER BY duration_sec DESC, id) row_seq,
        ROW_NUMBER() OVER (ORDER BY prev_event_time DESC, id) seq_no,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY prev_event_time, id) seq_no_2,
        LAG(last7_sum_duration_sec, 1) OVER (PARTITION BY page ORDER BY last7_sum_duration_sec, id) prev_last7_sum_duration_sec
    FROM islands
    WINDOW w_grp AS (PARTITION BY event_type ORDER BY session_no)
),
with_dims AS (
    -- Attach sessions attributes
    SELECT a.*, p.country
    FROM metrics a
    INNER JOIN sessions p
        ON a.session_id = p.id
    WHERE p.device = 'tablet'
),
joined AS (
    -- Attach sessions attributes
    SELECT a.*, s.device
    FROM with_dims a
    INNER JOIN sessions s
        ON a.session_id = s.id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN row_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_tier,
        CASE WHEN duration_sec >= 500 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
        ROUND(AVG(last7_avg_duration_sec), 2) AS avg_last7_avg_duration_sec,
        AVG(last7_count_duration_sec) AS avg_last7_count_duration_sec,
        COUNT(*) AS row_count,
        RANK() OVER (PARTITION BY CASE WHEN row_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY ROUND(AVG(last7_avg_duration_sec), 2) DESC) AS avg_last7_avg_duration_sec_rank
    FROM joined
    GROUP BY 1, 2
)
SELECT
    row_seq_tier,
    duration_sec_tier,
    avg_last7_count_duration_sec
FROM daily
