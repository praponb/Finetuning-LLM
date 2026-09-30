WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        ABS(ms_played - 176302) AS ms_played_distance
    FROM plays
    WHERE device = 'phone'
      AND track_id >= 24
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.device IN ('phone', 'speaker', 'car')
        )
),
unique_rows AS (
    -- Keep the latest row per listener_id, source
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        ms_played_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id, source ORDER BY ms_played_distance DESC, id DESC) AS row_num
        FROM without_matches
    ) ranked
    WHERE row_num = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        ms_played,
        is_skipped,
        source,
        NTILE(4) OVER (PARTITION BY is_skipped ORDER BY ms_played_distance ASC, id) AS ms_played_distance_quartile,
        COUNT(*) OVER (PARTITION BY listener_id, source) AS rows_in_listener_id_source
    FROM unique_rows
),
without_matches_2 AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND TO_DATE(p.played_at) < DATE '2025-02-12'
              AND p.device <> 'phone'
        )
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.ms_played_distance_quartile,
        s.rows_in_listener_id_source,
        l.interests AS listener_interests,
        l.plan AS listener_plan,
        l.username AS listener_username
    FROM without_matches_2 s
    JOIN listeners l ON l.id = s.listener_id
)
-- Final projection
SELECT
    id,
    listener_id,
    ms_played,
    is_skipped,
    source,
    ms_played_distance_quartile,
    listener_plan,
    listener_username
FROM with_details_listener
WHERE rows_in_listener_id_source < 12
ORDER BY id
