WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(page, ' ', event_type) AS page_event_type
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-06-18 22:00:00'
      AND page IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT page
    FROM valid
    INTERSECT ALL
    SELECT page
    FROM web_events
    WHERE event_type <> 'purchase'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.page = k.page
),
enriched AS (
    -- Attach sessions attributes
    SELECT src.*, s.country
    FROM reconciled src
    LEFT JOIN sessions s
        ON src.session_id = s.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(duration_sec) OVER (PARTITION BY country) AS country_stddev_samp_duration_sec,
        RANK() OVER (PARTITION BY TO_DATE(event_time) ORDER BY duration_sec NULLS LAST) AS duration_sec_rank,
        RANK() OVER (ORDER BY duration_sec NULLS LAST) AS duration_sec_rank_2,
        SUM(duration_sec) OVER w_hist AS duration_sec_sum_30d
    FROM enriched
    WINDOW w_hist AS (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
),
metrics_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        event_time,
        page_event_type,
        country_stddev_samp_duration_sec,
        duration_sec_rank_2,
        duration_sec_sum_30d,
        SUM(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_sum_90d,
        CUME_DIST() OVER (PARTITION BY page ORDER BY duration_sec_rank_2 DESC) AS duration_sec_rank_2_cume_dist,
        STDDEV_SAMP(country_stddev_samp_duration_sec) OVER (PARTITION BY MONTH(event_time), page) AS event_time_month_stddev_samp_country_stddev_samp_duration_sec,
        MAX(country_stddev_samp_duration_sec) OVER (PARTITION BY TO_DATE(event_time)) AS event_time_to_date_max_country_stddev_samp_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY TO_DATE(event_time) ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_max_30d
    FROM metrics
),
joined AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.country_stddev_samp_duration_sec,
        e.duration_sec_rank_2,
        e.duration_sec_max_30d,
        p.browser
    FROM metrics_web_events e
    LEFT JOIN sessions p
        ON e.session_id = p.id
    WHERE p.device = 'tablet'
)
SELECT
    id,
    duration_sec_rank_2,
    browser
FROM joined
ORDER BY duration_sec_rank_2 NULLS LAST;
