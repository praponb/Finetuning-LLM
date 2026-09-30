WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE artist_id >= 12
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit, artist_id), 0), 4) AS bpm_share_of_is_explicit_artist_id,
        DENSE_RANK() OVER w_is_explicit_release_date_mon AS duration_sec_dense_rank,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY is_explicit, artist_id), 2) AS duration_sec_vs_is_explicit_artist_id_avg,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, MONTH(release_date) ORDER BY duration_sec, id) AS rn,
        LAST_VALUE(bpm) OVER (PARTITION BY is_explicit, MONTH(release_date) ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm
    FROM scoped_tracks
    WINDOW w_is_explicit_release_date_mon AS (PARTITION BY is_explicit, MONTH(release_date) ORDER BY duration_sec, id)
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.moods,
        s.bpm_share_of_is_explicit_artist_id,
        s.duration_sec_dense_rank,
        s.rn,
        s.last_bpm,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at
    FROM ranked s
    JOIN plays p ON p.track_id = s.id
),
joined_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.moods,
        s.bpm_share_of_is_explicit_artist_id,
        s.duration_sec_dense_rank,
        s.rn,
        s.play_id,
        s.play_is_skipped,
        s.play_ms_played,
        s.play_played_at,
        a.is_verified AS artist_is_verified,
        p.played_at AS play_played_at_2,
        p.source AS play_source
    FROM line_level_plays s
    JOIN artists a ON a.id = s.artist_id
    LEFT JOIN plays p ON p.id = s.play_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods,
        bpm_share_of_is_explicit_artist_id,
        rn,
        play_id,
        play_ms_played,
        play_played_at,
        play_played_at_2,
        play_source,
        LENGTH(TRIM(play_source)) AS play_source_len,
        HOUR(play_played_at_2) AS play_played_at_2_hour,
        DATE_FORMAT(play_played_at, 'MMM yyyy') AS play_played_at_label
    FROM joined_artist
)
-- Subtotals with CUBE
SELECT
    artist_id,
    play_id,
    is_explicit,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched
GROUP BY CUBE(artist_id, play_id, is_explicit)
ORDER BY artist_id NULLS LAST, play_id NULLS LAST, is_explicit NULLS LAST
