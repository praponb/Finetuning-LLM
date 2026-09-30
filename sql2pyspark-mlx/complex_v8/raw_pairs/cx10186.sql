WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source
    FROM plays
),
with_details_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        t.bpm AS track_bpm,
        a.genre AS artist_genre
    FROM filtered_plays s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        source,
        ROUND(PERCENT_RANK() OVER w_all_by_track_bpm, 3) AS track_bpm_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id ORDER BY track_bpm NULLS LAST), 3) AS track_bpm_cume_dist,
        MAX(track_bpm) OVER (PARTITION BY track_id, artist_genre) AS track_id_artist_genre_max_track_bpm,
        track_bpm - LAG(track_bpm) OVER (ORDER BY track_bpm DESC, id) AS track_bpm_change
    FROM with_details_track
    WINDOW w_all_by_track_bpm AS (ORDER BY track_bpm DESC, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    source,
    track_bpm_percent_rank,
    track_bpm_cume_dist,
    track_id_artist_genre_max_track_bpm,
    track_bpm_change,
    LENGTH(TRIM(source)) AS source_len,
    COALESCE(track_bpm_change, 0) AS track_bpm_change_filled
FROM with_windows
ORDER BY listener_id DESC, id
LIMIT 10
