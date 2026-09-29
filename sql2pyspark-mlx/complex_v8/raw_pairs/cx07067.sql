WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at,
        ABS(ms_played - 239955) AS ms_played_distance,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE is_skipped = FALSE
      AND source IN ('album', 'radio')
),
source_stats AS (
    -- Max ms_played per source
    SELECT source, MAX(ms_played) AS max_ms_played
    FROM base
    GROUP BY source
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ms_played,
        ROUND(s.ms_played - g.max_ms_played, 2) AS ms_played_minus_max
    FROM base s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played = g.max_ms_played
),
joined_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.days_since_played_at,
        s.ms_played_distance,
        s.max_ms_played,
        l.country AS listener_country,
        l.birth_year AS listener_birth_year
    FROM outliers s
    JOIN listeners l ON l.id = s.listener_id AND l.country = 'BR'
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at_2
    FROM joined_listener s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT first_played_at_2, source, listener_country, listener_birth_year
    FROM profiled
) src
PIVOT (
    AVG(listener_birth_year)
    FOR listener_country IN ('UK', 'TH', 'BR', 'KR')
)
ORDER BY first_played_at_2, source
