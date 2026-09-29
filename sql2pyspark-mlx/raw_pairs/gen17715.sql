WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        CEIL(duration_sec) AS duration_sec_whole,
        DAY(event_time) AS event_time_day,
        COALESCE(page, 'OTHER') AS page_filled
    FROM web_events
    WHERE event_time >= TO_DATE('2023-10-02')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        duration_sec_whole,
        event_time_day,
        AVG(event_time_day) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_event_time_day,
        COUNT(event_time_day) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_event_time_day,
        MAX(event_time_day) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_event_time_day,
        AVG(duration_sec_whole) OVER (PARTITION BY session_id, event_type ORDER BY duration_sec, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS duration_sec_whole_avg_5_prior
    FROM cleaned
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY running_max_event_time_day NULLS FIRST) AS running_max_event_time_day_percent_rank,
        ROW_NUMBER() OVER (ORDER BY duration_sec_whole DESC, id) AS row_seq,
        MAX(duration_sec_whole_avg_5_prior) OVER (PARTITION BY session_id) AS session_id_max_duration_sec_whole_avg_5_prior
    FROM with_windows
),
joined AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.duration_sec_whole,
        s.event_time_day,
        s.running_count_event_time_day,
        s.running_max_event_time_day,
        s.running_max_event_time_day_percent_rank,
        s.row_seq,
        s.session_id_max_duration_sec_whole_avg_5_prior,
        p.browser,
        p.device
    FROM scored s
    LEFT JOIN sessions p
        ON s.session_id = p.id
    WHERE p.started_at >= '2024-12-11'
),
cleaned_web_events AS (
    -- Keep recent, valid rows
    SELECT *, session_id_max_duration_sec_whole_avg_5_prior - running_max_event_time_day AS session_id_max_duration_sec_whole_avg_5_prior_minus_running_max_event_time_day
    FROM joined
    WHERE browser IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned_web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY browser ORDER BY session_id_max_duration_sec_whole_avg_5_prior DESC NULLS FIRST, id) = 1
)
-- Multi-level aggregation
SELECT
    browser,
    CASE WHEN running_count_event_time_day >= 17 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_count_event_time_day_tier,
    AVG(row_seq) FILTER (WHERE device = 'mobile') AS mobile_avg_row_seq,
    COUNT(*) FILTER (WHERE device = 'mobile') AS mobile_rows,
    SUM(CASE WHEN browser = 'Chrome' THEN running_max_event_time_day ELSE 0 END) AS chrome_running_max_event_time_day,
    SUM(CASE WHEN browser = 'Safari' THEN running_max_event_time_day_percent_rank ELSE 0 END) AS safari_running_max_event_time_day_percent_rank
FROM deduped
GROUP BY 1, 2
ORDER BY mobile_rows DESC NULLS FIRST
