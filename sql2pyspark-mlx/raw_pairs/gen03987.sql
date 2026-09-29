WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER w_hist AS duration_sec_percent_rank,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_avg_3,
        STDDEV_SAMP(duration_sec) OVER (PARTITION BY event_type, session_id) AS event_type_stddev_samp_duration_sec
    FROM web_events
    WINDOW w_hist AS (PARTITION BY MONTH(event_time) ORDER BY duration_sec DESC NULLS LAST)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        page,
        duration_sec,
        duration_sec_percent_rank,
        duration_sec_avg_3,
        event_type_stddev_samp_duration_sec,
        ROW_NUMBER() OVER (ORDER BY duration_sec DESC, id) AS row_seq,
        CUME_DIST() OVER (PARTITION BY event_type ORDER BY duration_sec NULLS LAST) AS duration_sec_cume_dist,
        CUME_DIST() OVER (ORDER BY duration_sec_avg_3 DESC) AS duration_sec_avg_3_cume_dist
    FROM scored
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        page,
        duration_sec,
        duration_sec_percent_rank,
        duration_sec_avg_3,
        event_type_stddev_samp_duration_sec,
        row_seq,
        duration_sec_cume_dist,
        duration_sec_avg_3_cume_dist,
        ROUND(event_type_stddev_samp_duration_sec, 1) AS event_type_stddev_samp_duration_sec_rounded
    FROM enriched
    WHERE page IS NOT NULL
      AND duration_sec != 196
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS latest_rank
        FROM filtered
    ) t
    WHERE latest_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        page,
        duration_sec_percent_rank,
        duration_sec_cume_dist,
        event_type_stddev_samp_duration_sec_rounded,
        duration_sec_avg_3_cume_dist
    FROM latest
) src
UNPIVOT (
    metric_value FOR metric IN (duration_sec_percent_rank, duration_sec_cume_dist, event_type_stddev_samp_duration_sec_rounded, duration_sec_avg_3_cume_dist)
);
