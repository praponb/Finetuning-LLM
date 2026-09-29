WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label,
        ABS(ms_played - 160264) AS ms_played_distance,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label_2
    FROM plays
    WHERE listener_id IS NOT NULL
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.source = 'playlist'
        )
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        CONCAT_WS('-', played_at_label, played_at_label_2) AS played_at_label_played_at_label_2_label,
        CONCAT(played_at_label, '#', CAST(id AS STRING)) AS played_at_label_ref
    FROM screened
),
with_parent AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.played_at_label,
        s.ms_played_distance,
        s.is_skipped_label,
        s.played_at_label_played_at_label_2_label,
        s.played_at_label_ref,
        l.username AS listener_username
    FROM tagged s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.ms_played_distance,
        s.listener_username,
        l.birth_year AS listener_birth_year,
        l.plan AS listener_plan,
        l.interests AS listener_interests
    FROM with_parent s
    JOIN listeners l ON l.id = s.listener_id
),
with_parent_2 AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.ms_played_distance,
        s.listener_username,
        s.listener_birth_year,
        s.listener_plan,
        s.listener_interests,
        l.username AS listener_username_2,
        l.birth_year AS listener_birth_year_2,
        l.country AS listener_country
    FROM with_details s
    JOIN listeners l ON l.id = s.listener_id
)
-- Subtotals with CUBE
SELECT
    listener_id,
    is_skipped,
    GROUPING_ID() AS gid,
    ROUND(AVG(listener_birth_year_2), 2) AS avg_listener_birth_year_2
FROM with_parent_2
GROUP BY CUBE(listener_id, is_skipped)
