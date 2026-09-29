WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        duration_sec + bpm AS duration_sec_plus_bpm
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'party')
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE p.device <> 'car') FROM plays p WHERE p.track_id = s.id) AS play_device_count
    FROM valid s
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.duration_sec_filled,
        s.duration_sec_plus_bpm,
        s.play_device_count,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source
    FROM summarized_rows s
    JOIN plays p ON p.track_id = s.id AND p.ms_played > 98052
)
SELECT *
FROM (
    SELECT is_explicit, release_date, play_source, bpm
    FROM exploded_detail
) src
PIVOT (
    AVG(bpm)
    FOR play_source IN ('radio', 'search')
)
ORDER BY is_explicit, release_date
