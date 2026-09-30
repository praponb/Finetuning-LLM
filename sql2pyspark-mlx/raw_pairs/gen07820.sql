WITH matched_keys AS (
    SELECT id
    FROM web_events
    INTERSECT
    SELECT id
    FROM web_events
    WHERE page IN ('/product', '/search')
),
kept_rows AS (
    SELECT s.*
    FROM web_events s
    INNER JOIN matched_keys k ON s.id = k.id
),
metrics AS (
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_avg_30d,
        RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS duration_sec_rank,
        COUNT(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_count_7d
    FROM kept_rows
),
joined AS (
    SELECT a.*, p.country
    FROM metrics a
    LEFT JOIN sessions p
        ON a.session_id = p.id
),
enriched AS (
    SELECT
        src.session_id,
        src.event_type,
        src.event_time,
        src.duration_sec,
        src.duration_sec_avg_30d,
        src.duration_sec_rank,
        src.duration_sec_count_7d,
        p.browser,
        p.country AS session_country
    FROM joined src
    LEFT JOIN sessions p
        ON src.session_id = p.id
),
base AS (
    SELECT
        session_id,
        event_type,
        event_time,
        duration_sec_avg_30d,
        duration_sec_rank,
        duration_sec_count_7d,
        browser,
        session_country,
        LOWER(event_type) AS event_type_lower
    FROM enriched
    WHERE event_time >= TIMESTAMP '2023-08-07 21:00:00'
      AND session_country IS NOT NULL
),
enriched_web_events AS (
    SELECT
        session_id,
        duration_sec_count_7d,
        browser,
        event_type_lower,
        COUNT(duration_sec_avg_30d) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_avg_30d_count_7d,
        PERCENT_RANK() OVER (ORDER BY duration_sec_count_7d) AS duration_sec_count_7d_percent_rank,
        MIN(duration_sec_count_7d) OVER (PARTITION BY event_type) AS event_type_min_duration_sec_count_7d,
        COUNT(duration_sec_rank) OVER (PARTITION BY session_country ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_rank_count_7d,
        ROUND(duration_sec_rank / SUM(duration_sec_rank) OVER (PARTITION BY session_country), 4) AS duration_sec_rank_share
    FROM base
)
SELECT *
FROM (
    SELECT
        session_id,
        duration_sec_rank_share,
        duration_sec_count_7d_percent_rank
    FROM enriched_web_events
) src
UNPIVOT (
    reading FOR kpi IN (duration_sec_rank_share, duration_sec_count_7d_percent_rank)
) u;
