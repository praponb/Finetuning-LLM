WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        CONCAT(source, '#', CAST(track_id AS STRING)) AS source_ref,
        CASE WHEN device IN ('phone', 'tv') THEN 'group_a' WHEN device = 'speaker' THEN 'group_b' ELSE 'other' END AS device_group,
        LOWER(TRIM(source)) AS source_clean
    FROM plays
    WHERE NOT is_skipped
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM recent s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.source = 'radio'
              AND p.device IN ('car', 'speaker', 'desktop')
        )
      AND s.device_group IS NOT NULL
),
with_parent_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.source_clean,
        l.birth_year AS listener_birth_year,
        l.signup_date AS listener_signup_date,
        l.country AS listener_country
    FROM screened s
    LEFT JOIN listeners l ON l.id = s.listener_id AND l.plan NOT IN ('premium', 'student')
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    source_clean,
    listener_birth_year,
    COALESCE(listener_country, 'UNKNOWN') AS listener_country_or_unknown
FROM with_parent_listener
ORDER BY listener_id
