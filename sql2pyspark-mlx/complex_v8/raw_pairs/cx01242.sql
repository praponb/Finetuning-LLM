WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        CONCAT_WS(' / ', source, device) AS source_device_label,
        INITCAP(TRIM(device)) AS device_clean,
        HOUR(played_at) AS played_at_hour
    FROM plays
    WHERE played_at < TIMESTAMP '2025-03-02 10:00:00'
      AND source <> 'playlist'
),
listener_id_stats AS (
    -- Avg played_at_hour per listener_id
    SELECT listener_id, AVG(played_at_hour) AS avg_played_at_hour
    FROM scoped
    GROUP BY listener_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_played_at_hour,
        ROUND(s.played_at_hour - g.avg_played_at_hour, 2) AS played_at_hour_minus_avg
    FROM scoped s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.played_at_hour > g.avg_played_at_hour
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN played_at_hour >= 12 THEN 'large' WHEN played_at_hour >= 8 THEN 'medium' ELSE 'small' END AS played_at_hour_tier,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM vs_group
    WHERE listener_id IS NOT NULL
),
joined_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        s.source_device_label,
        s.device_clean,
        s.played_at_hour_minus_avg,
        s.source_slug,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year
    FROM scored s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
with_activity AS (
    -- Keep rows whose track matches
    SELECT s.*
    FROM joined_listener s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.id = s.track_id
              AND t.bpm > 76
              AND NOT t.is_explicit
        )
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    is_skipped,
    source,
    played_at_hour_minus_avg,
    source_slug,
    listener_birth_year
FROM with_activity
ORDER BY id, played_at NULLS LAST
