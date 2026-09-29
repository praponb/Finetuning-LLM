WITH recent_plays AS (
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
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label
    FROM plays
    WHERE (device NOT IN ('phone', 'desktop') OR ms_played >= 107912)
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM recent_plays s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.device IN ('tv', 'car', 'desktop')
              AND p.ms_played < 223262
        )
      AND s.played_at < TIMESTAMP '2025-02-24 11:00:00'
),
combined_details AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.played_at_label,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year
    FROM with_activity s
    JOIN listeners l ON l.id = s.listener_id
    WHERE NOT ARRAY_CONTAINS(l.interests, 'kpop')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_track_id_by_ms_played, 3) AS ms_played_cume_dist,
        LAG(ms_played, 3, 0) OVER (PARTITION BY track_id ORDER BY ms_played ASC, id) AS prev3_ms_played
    FROM combined_details
    WINDOW w_track_id_by_ms_played AS (PARTITION BY track_id ORDER BY ms_played ASC, id)
),
latest AS (
    -- Keep the earliest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        played_at_label,
        listener_interests,
        listener_birth_year,
        ms_played_cume_dist,
        prev3_ms_played
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY listener_birth_year, id) AS rn
        FROM sequenced
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    played_at_label,
    listener_birth_year,
    ms_played_cume_dist,
    ABS(ms_played - 129336) AS ms_played_distance
FROM latest
WHERE ms_played_cume_dist > 0.1
