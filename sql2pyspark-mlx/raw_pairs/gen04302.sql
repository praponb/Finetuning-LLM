WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(duration_sec * 1.15, 2) duration_sec_scaled,
        CAST(duration_sec AS STRING) duration_sec_num,
        COALESCE(page, 'UNKNOWN') page_filled
    FROM web_events
    WHERE page IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        page_filled,
        TO_DATE(event_time) event_time_date
    FROM valid
    WHERE page IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(duration_sec) OVER w_grp prev_duration_sec,
        ROW_NUMBER() OVER w_grp event_seq,
        SUM(duration_sec) OVER (PARTITION BY YEAR(event_time_date), session_id) event_time_date_year_sum_duration_sec
    FROM filtered
    WINDOW w_grp AS (PARTITION BY event_type ORDER BY event_time, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(event_time_date_year_sum_duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_event_time_date_year_sum_duration_sec,
        SUM(event_time_date_year_sum_duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_event_time_date_year_sum_duration_sec,
        MAX(event_time_date_year_sum_duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_max_event_time_date_year_sum_duration_sec,
        AVG(event_seq) OVER (PARTITION BY event_type ORDER BY prev_duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) event_seq_avg_6_prior
    FROM enriched
),
enriched_web_events AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_recent duration_sec_dense_rank,
        NTILE(10) OVER (PARTITION BY event_type ORDER BY last7_avg_event_time_date_year_sum_duration_sec, id) last7_avg_event_time_date_year_sum_duration_sec_bucket,
        SUM(duration_sec) OVER (PARTITION BY session_id) session_id_sum_duration_sec
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY event_type ORDER BY duration_sec DESC NULLS LAST)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('YEAR', event_time_date) AS year_start,
        SUM(duration_sec_dense_rank) FILTER (WHERE event_type = 'page_view') AS page_view_total_duration_sec_dense_rank,
        SIZE(COLLECT_SET(event_type)) AS num_event_types
    FROM enriched_web_events
    GROUP BY DATE_TRUNC('YEAR', event_time_date)
)
SELECT
    year_start,
    page_view_total_duration_sec_dense_rank,
    num_event_types
FROM stats;
