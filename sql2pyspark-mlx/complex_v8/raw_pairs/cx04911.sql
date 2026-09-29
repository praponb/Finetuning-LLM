WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 5
      AND release_date >= DATE '2024-08-26'
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        moods_text,
        bpm_per_duration_sec,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix,
        CONCAT(title, '#', CAST(duration_sec_filled AS STRING)) AS title_ref,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM filtered_tracks
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.duration_sec,
        s.is_explicit,
        s.moods,
        s.moods_text,
        s.bpm_per_duration_sec,
        s.title_prefix,
        s.bpm_bucket,
        p.id AS play_id,
        p.device AS play_device,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at
    FROM enriched s
    LEFT JOIN plays p ON p.track_id = s.id
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.duration_sec,
        s.is_explicit,
        s.moods_text,
        s.bpm_per_duration_sec,
        s.title_prefix,
        s.bpm_bucket,
        s.play_id,
        s.play_device,
        s.play_ms_played,
        s.play_is_skipped,
        s.play_played_at,
        t.mood
    FROM with_children_plays s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
)
-- Subtotals with ROLLUP
SELECT
    mood,
    play_id,
    ROUND(STDDEV(play_ms_played), 2) AS stddev_play_ms_played
FROM per_label
GROUP BY ROLLUP(mood, play_id)
ORDER BY mood NULLS LAST, play_id NULLS LAST
