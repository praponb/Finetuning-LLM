WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'C%'
      AND duration_sec > 142
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.moods,
        s.duration_sec_filled,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at
    FROM scoped_tracks s
    JOIN plays p ON p.track_id = s.id AND p.played_at >= TIMESTAMP '2025-02-06 12:00:00'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(title)) AS title_len,
        duration_sec_filled - bpm AS duration_sec_filled_minus_bpm
    FROM detail_rows_plays
    WHERE artist_id <= 8
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    release_date,
    moods,
    duration_sec_filled,
    play_id,
    play_ms_played,
    play_is_skipped,
    play_played_at,
    title_len,
    duration_sec_filled_minus_bpm
FROM labeled
ORDER BY title_len DESC NULLS FIRST
