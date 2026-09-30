WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        UPPER(TRIM(source)) AS source_clean,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE source = 'search'
      AND is_skipped = TRUE
),
enriched_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.ms_played_filled,
        s.source_clean,
        l.birth_year AS listener_birth_year,
        l.plan AS listener_plan
    FROM scoped s
    JOIN listeners l ON l.id = s.listener_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(listener_birth_year, 0) AS listener_birth_year_filled,
        FLOOR(listener_birth_year / 10) * 10 AS listener_birth_year_bucket,
        ms_played_filled + ms_played AS ms_played_filled_plus_ms_played
    FROM enriched_listener
),
track_id_stats AS (
    -- Avg listener_birth_year per track_id
    SELECT track_id, AVG(listener_birth_year) AS avg_listener_birth_year
    FROM derived
    GROUP BY track_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_listener_birth_year,
        ROUND(s.listener_birth_year - g.avg_listener_birth_year, 2) AS listener_birth_year_minus_avg
    FROM derived s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.listener_birth_year > g.avg_listener_birth_year
)
SELECT *
FROM (
    SELECT listener_plan, track_id, source, ms_played_filled
    FROM above_average
) src
PIVOT (
    MAX(ms_played_filled) AS max_v, SUM(ms_played_filled) AS sum_v
    FOR source IN ('search', 'playlist', 'album', 'radio')
)
ORDER BY listener_plan, track_id
