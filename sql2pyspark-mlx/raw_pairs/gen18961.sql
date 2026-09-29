WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        ROUND(duration_sec * 1.07, 2) AS duration_sec_adjusted
    FROM web_events
    WHERE page IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        page,
        duration_sec_adjusted,
        LAST_VALUE(page) IGNORE NULLS OVER w_hist AS last_known_page,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        ROW_NUMBER() OVER (ORDER BY event_time, id) AS seq_no,
        LEAD(duration_sec_adjusted, 1, 0) OVER (ORDER BY duration_sec_adjusted, id) AS next_duration_sec_adjusted,
        STDDEV_SAMP(duration_sec_adjusted) OVER (PARTITION BY event_type) AS event_type_stddev_samp_duration_sec_adjusted
    FROM filtered
    WINDOW w_hist AS (ORDER BY duration_sec_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.page,
        e.cumulative_duration_sec,
        e.seq_no,
        e.next_duration_sec_adjusted,
        b.country
    FROM with_windows e
    LEFT JOIN sessions b
        ON e.session_id = b.id
),
with_windows_web_events AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(cumulative_duration_sec) OVER w_grp AS running_count_cumulative_duration_sec,
        AVG(cumulative_duration_sec) OVER w_grp AS running_avg_cumulative_duration_sec,
        SUM(cumulative_duration_sec) OVER w_grp AS running_sum_cumulative_duration_sec
    FROM joined
    WINDOW w_grp AS (PARTITION BY page, session_id ORDER BY next_duration_sec_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined_web_events AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.page,
        s.cumulative_duration_sec,
        s.seq_no,
        s.country,
        s.running_avg_cumulative_duration_sec,
        s.running_sum_cumulative_duration_sec,
        p2.device,
        p2.country AS session_country
    FROM with_windows_web_events s
    JOIN sessions p2
        ON s.session_id = p2.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        page,
        cumulative_duration_sec,
        country,
        running_sum_cumulative_duration_sec,
        device,
        session_country,
        AVG(running_sum_cumulative_duration_sec) OVER (PARTITION BY device, session_id ORDER BY running_avg_cumulative_duration_sec, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS running_sum_cumulative_duration_sec_avg_5,
        ROW_NUMBER() OVER (ORDER BY cumulative_duration_sec DESC, id) AS event_seq,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM joined_web_events
    WINDOW w_grp AS (ORDER BY running_sum_cumulative_duration_sec, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(event_seq) OVER w_part AS last7_count_event_seq,
        MAX(event_seq) OVER w_part AS last7_max_event_seq,
        AVG(event_seq) OVER w_part AS last7_avg_event_seq,
        SUM(running_sum_cumulative_duration_sec) OVER (PARTITION BY session_country ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_sum_cumulative_duration_sec
    FROM enriched
    WINDOW w_part AS (PARTITION BY page ORDER BY cumulative_duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    page,
    cumulative_duration_sec,
    country,
    running_sum_cumulative_duration_sec,
    device,
    running_sum_cumulative_duration_sec_avg_5,
    event_seq,
    last7_count_event_seq,
    CASE WHEN device RLIKE '^(tablet|desktop)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS device_zone
FROM metrics
ORDER BY cumulative_duration_sec
