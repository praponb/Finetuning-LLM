WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, page
    FROM web_events
    EXCEPT
    SELECT id, page
    FROM web_events
    WHERE page = '/home'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    JOIN kept_keys k ON s.id = k.id AND s.page = k.page
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_max_90d,
        STDDEV_SAMP(duration_sec) OVER (PARTITION BY page) AS page_stddev_samp_duration_sec
    FROM kept_rows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY session_id ORDER BY duration_sec_max_90d ASC) AS duration_sec_max_90d_cume_dist,
        AVG(duration_sec_max_90d) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_max_90d_avg_7d
    FROM enriched
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        duration_sec_max_90d,
        page_stddev_samp_duration_sec,
        duration_sec_max_90d_cume_dist,
        duration_sec_max_90d_avg_7d,
        AVG(duration_sec) OVER (PARTITION BY session_id, event_type ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_avg_90d,
        SUM(duration_sec_max_90d_cume_dist) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_max_90d_cume_dist_sum_90d
    FROM metrics
)
SELECT
    event_type,
    page,
    event_time,
    duration_sec,
    duration_sec_max_90d,
    page_stddev_samp_duration_sec,
    duration_sec_max_90d_cume_dist,
    duration_sec_max_90d_avg_7d,
    duration_sec_avg_90d
FROM scored
ORDER BY duration_sec_max_90d_cume_dist ASC;
