WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(event_type, 1, 4) AS event_type_prefix,
        UPPER(page) AS page_upper
    FROM web_events
    WHERE event_time >= DATE '2023-09-02'
      AND page IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        page_upper,
        SUM(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        LAST_VALUE(page) IGNORE NULLS OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_page
    FROM cleaned
),
with_windows_web_events AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS row_seq,
        SUM(cumulative_duration_sec) OVER (PARTITION BY TO_DATE(event_time), session_id ORDER BY cumulative_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_duration_sec,
        SUM(cumulative_duration_sec) OVER (PARTITION BY DATE_TRUNC('YEAR', event_time), session_id ORDER BY cumulative_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cumulative_duration_sec,
        AVG(cumulative_duration_sec) OVER (PARTITION BY YEAR(event_time) ORDER BY event_time, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cumulative_duration_sec_avg_5_prior,
        AVG(cumulative_duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_avg_7d
    FROM with_windows
    WHERE event_type IN ('page_view', 'search')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        cumulative_duration_sec,
        last_known_page,
        running_cumulative_duration_sec,
        cumulative_duration_sec_avg_5_prior,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY event_type), 4) AS row_seq_share,
        ROW_NUMBER() OVER (PARTITION BY session_id, event_type ORDER BY cumulative_cumulative_duration_sec, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC, id) AS row_seq_2,
        COUNT(cumulative_duration_sec) OVER (PARTITION BY session_id, event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_count_7d,
        SUM(running_cumulative_duration_sec) OVER (PARTITION BY event_type ORDER BY cumulative_cumulative_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_cumulative_duration_sec
    FROM with_windows_web_events
)
-- Multi-level aggregation
SELECT
    CASE WHEN cumulative_duration_sec_avg_5_prior >= 2221 THEN 'HIGH' ELSE 'LOW' END AS cumulative_duration_sec_avg_5_prior_tier,
    SUM(CASE WHEN event_type = 'click' THEN cumulative_duration_sec ELSE 0 END) AS click_cumulative_duration_sec,
    COUNT(*) FILTER (WHERE event_type = 'search') AS search_rows,
    ROUND(AVG(row_seq_share), 2) AS avg_row_seq_share,
    ROUND(AVG(row_seq_share), 2) AS avg_row_seq_share_2,
    MAX(event_time) AS ended_at
FROM metrics
GROUP BY CASE WHEN cumulative_duration_sec_avg_5_prior >= 2221 THEN 'HIGH' ELSE 'LOW' END
ORDER BY search_rows;
