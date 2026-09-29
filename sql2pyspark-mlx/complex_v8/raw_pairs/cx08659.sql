WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        ABS(ms_played - 107912) AS ms_played_distance,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-02-06 15:00:00'
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.ms_played_distance,
        s.played_at_label,
        p.id AS play_id,
        p.device AS play_device,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped
    FROM recent_plays s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at,
        device,
        is_skipped,
        ms_played_distance,
        play_id,
        play_device,
        play_ms_played,
        play_is_skipped,
        LAG(play_ms_played) OVER (PARTITION BY device ORDER BY played_at DESC NULLS FIRST, id, play_id) AS prev_play_ms_played,
        ROUND(ms_played_distance - AVG(ms_played_distance) OVER (PARTITION BY is_skipped), 2) AS ms_played_distance_vs_is_skipped_avg,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY is_skipped)) AS distinct_track_id_in_is_skipped
    FROM exploded_detail_plays
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year,
        l.interests AS listener_interests
    FROM ranked s
    JOIN listeners l ON l.id = s.listener_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, play_id, metric, reading
FROM (
    SELECT id, play_id, rows_in_is_skipped, listener_birth_year
    FROM enriched
) src
UNPIVOT (
    reading FOR metric IN (rows_in_is_skipped, listener_birth_year)
) unp
ORDER BY id, play_id, metric
