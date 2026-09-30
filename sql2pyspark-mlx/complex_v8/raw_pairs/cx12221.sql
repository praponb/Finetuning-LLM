WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE release_date >= DATE '2024-05-12'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        duration_sec + bpm AS duration_sec_plus_bpm,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        SUBSTRING(TRIM(title_prefix), 1, 4) AS title_prefix_prefix
    FROM recent_tracks
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played_2
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.avg_play_ms_played,
        p.avg_play_ms_played_2
    FROM scored s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.moods,
        s.title_prefix,
        s.duration_sec_plus_bpm,
        s.avg_play_ms_played,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played
    FROM profiled s
    LEFT JOIN plays p ON p.track_id = s.id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        bpm,
        moods,
        title_prefix,
        duration_sec_plus_bpm,
        avg_play_ms_played,
        play_id,
        play_played_at,
        play_listener_id,
        ROUND(bpm - AVG(bpm) OVER w_artist_id, 2) AS bpm_vs_artist_id_avg,
        ROUND(AVG(avg_play_ms_played) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_avg_play_ms_played
    FROM detail_rows
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.title_prefix,
        s.duration_sec_plus_bpm,
        s.avg_play_ms_played,
        s.play_id,
        s.play_played_at,
        s.play_listener_id,
        s.bpm_vs_artist_id_avg,
        s.artist_id_avg_avg_play_ms_played,
        a.genre AS artist_genre,
        l.plan AS listener_plan
    FROM sequenced s
    LEFT JOIN artists a ON a.id = s.artist_id
    JOIN listeners l ON l.id = s.play_listener_id
)
SELECT
    id,
    bpm,
    title_prefix,
    duration_sec_plus_bpm,
    avg_play_ms_played,
    play_played_at,
    bpm_vs_artist_id_avg,
    artist_id_avg_avg_play_ms_played,
    artist_genre
FROM combined_details_artist
ORDER BY bpm DESC
