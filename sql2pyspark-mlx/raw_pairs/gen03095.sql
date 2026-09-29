WITH with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
with_gaps AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessionized AS (
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
metrics AS (
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS seq_no
    FROM sessionized
    WHERE burst_id < 6
),
flagged AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM metrics
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
with_windows AS (
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        running_count_duration_sec,
        running_max_duration_sec,
        seq_no,
        LAG(island_id, 2, 0) OVER (PARTITION BY session_id ORDER BY is_new_burst, id) AS prev_island_id,
        ROW_NUMBER() OVER w_grp AS event_seq,
        DENSE_RANK() OVER (PARTITION BY session_id ORDER BY running_max_duration_sec DESC NULLS LAST) AS running_max_duration_sec_dense_rank
    FROM islands
    WINDOW w_grp AS (PARTITION BY session_id ORDER BY running_count_duration_sec, id)
),
ranked AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY running_count_duration_sec DESC, id) = 1
),
flagged_2 AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM ranked
),
sessions_web_events AS (
    SELECT
        id,
        session_id,
        event_time,
        running_max_duration_sec,
        prev_island_id,
        running_max_duration_sec_dense_rank,
        SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged_2
),
joined AS (
    SELECT
        s.*,
        d.device,
        d.country,
        d.browser
    FROM sessions_web_events s
    LEFT JOIN sessions d
        ON s.session_id = d.id
)
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    browser,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    COUNT(CASE WHEN device = 'desktop' THEN 1 END) AS desktop_count,
    SIZE(COLLECT_SET(device)) AS num_devices
FROM joined
GROUP BY 1, 2
ORDER BY browser DESC
