WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(duration_sec, 2) AS duration_sec_rounded
    FROM web_events
    WHERE page IN ('/home', '/search', '/cart')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        event_type,
        CUME_DIST() OVER (PARTITION BY page ORDER BY duration_sec_rounded DESC) AS duration_sec_rounded_cume_dist,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec_rounded, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_3_prior,
        LEAD(duration_sec_rounded, 2) OVER (PARTITION BY YEAR(event_time), page ORDER BY duration_sec, id) AS next_duration_sec_rounded
    FROM base
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec_avg_3_prior DESC, id) = 1
),
ranked_web_events AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        event_type,
        duration_sec_rounded_cume_dist,
        duration_sec_avg_3_prior,
        next_duration_sec_rounded
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec_rounded_cume_dist DESC, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(duration_sec_avg_3_prior) OVER (PARTITION BY event_type ORDER BY duration_sec_rounded_cume_dist, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_3_prior_avg_3_prior,
        LAG(duration_sec_rounded_cume_dist, 1) OVER (PARTITION BY event_type ORDER BY next_duration_sec_rounded, id) AS prev_duration_sec_rounded_cume_dist,
        PERCENT_RANK() OVER (ORDER BY duration_sec_rounded_cume_dist NULLS LAST) AS duration_sec_rounded_cume_dist_percent_rank
    FROM ranked_web_events
    WHERE next_duration_sec_rounded BETWEEN 22 AND 31
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    event_type,
    metric_name,
    reading
FROM (
    SELECT
        id,
        event_type,
        duration_sec_rounded_cume_dist,
        duration_sec_rounded_cume_dist_percent_rank
    FROM scored
) src
UNPIVOT (
    reading FOR metric_name IN (duration_sec_rounded_cume_dist, duration_sec_rounded_cume_dist_percent_rank)
);
