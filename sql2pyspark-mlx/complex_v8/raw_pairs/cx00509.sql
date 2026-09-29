WITH valid AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        country,
        birth_year,
        interests,
        LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug
    FROM listeners
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        username,
        country,
        birth_year,
        interests,
        country_slug,
        CONCAT(username, '#', CAST(birth_year AS STRING)) AS username_ref
    FROM valid
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND p.played_at < TIMESTAMP '2025-02-12 11:00:00'
              AND p.is_skipped = FALSE
        )
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        username,
        country,
        birth_year,
        interests,
        username_ref,
        SIZE(interests) AS interests_count,
        LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug_2
    FROM without_matches
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.country,
        s.interests,
        s.username_ref,
        s.interests_count,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device
    FROM enriched s
    LEFT JOIN plays p ON p.listener_id = s.id
)
-- Final projection
SELECT DISTINCT play_id, id, interests_count
FROM detail_rows
ORDER BY id DESC NULLS FIRST, play_id
