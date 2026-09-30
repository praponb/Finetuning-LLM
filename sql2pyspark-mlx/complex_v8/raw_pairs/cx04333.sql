WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE ms_played >= 56848
      AND played_at < TIMESTAMP '2025-03-02 09:00:00'
),
enriched AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        l.plan AS listener_plan,
        l.country AS listener_country,
        l.birth_year AS listener_birth_year
    FROM valid s
    JOIN listeners l ON l.id = s.listener_id AND l.username IS NOT NULL
    WHERE l.country IS NOT NULL
),
with_parent_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.listener_plan,
        s.listener_birth_year,
        l.plan AS listener_plan_2,
        l.country AS listener_country_2,
        l.interests AS listener_interests
    FROM enriched s
    JOIN listeners l ON l.id = s.listener_id
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, l.country AS listener_country, l.username AS listener_username
    FROM with_parent_listener s
    LEFT JOIN listeners l ON l.id = s.listener_id
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    is_skipped,
    source,
    listener_plan,
    listener_birth_year,
    listener_plan_2,
    listener_country_2,
    listener_interests,
    INITCAP(TRIM(listener_country_2)) AS listener_country_2_clean
FROM joined
ORDER BY listener_country_2 DESC, id
LIMIT 5
