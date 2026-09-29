WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
    WHERE source <> 'album'
),
joined_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.is_skipped_label,
        l.plan AS listener_plan,
        l.birth_year AS listener_birth_year,
        l.interests AS listener_interests,
        t.moods AS track_moods
    FROM base s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
    WHERE l.plan IN ('student', 'family', 'free')
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at
    FROM joined_listener s
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.played_at >= TIMESTAMP '2025-02-06 08:00:00'
              AND p.ms_played BETWEEN 95960 AND 359437
        )
)
SELECT
    track_id,
    is_skipped_label,
    SUM(CASE WHEN NOT is_skipped THEN 1 ELSE 0 END) AS n_not_is_skipped,
    MAX(first_played_at) AS last_first_played_at
FROM with_activity
GROUP BY GROUPING SETS ((track_id, is_skipped_label), (track_id), ())
ORDER BY track_id NULLS LAST, is_skipped_label NULLS LAST
