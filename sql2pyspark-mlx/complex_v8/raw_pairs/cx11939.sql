WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'M%'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.artist_genre,
        s.artist_is_verified,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device,
        p.listener_id AS play_listener_id,
        p.played_at AS play_played_at
    FROM joined s
    LEFT JOIN plays p ON p.track_id = s.id
),
with_details AS (
    -- Attach play attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.artist_is_verified,
        s.play_id,
        s.play_source,
        s.play_listener_id,
        s.play_played_at,
        p.device AS play_device_2,
        p.track_id AS play_track_id,
        t.bpm AS track_bpm
    FROM with_children_plays s
    JOIN plays p ON p.id = s.play_id AND p.device IS NOT NULL
    LEFT JOIN tracks t ON t.id = p.track_id
)
-- Subtotals with ROLLUP
SELECT
    play_id,
    play_source,
    play_device_2,
    GROUPING_ID() AS grouping_level,
    MIN(play_played_at) AS first_play_played_at,
    ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(title)) LIKE 'C%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_title_like_c
FROM with_details
GROUP BY ROLLUP(play_id, play_source, play_device_2)
ORDER BY play_id NULLS LAST, play_source NULLS LAST, play_device_2 NULLS LAST
