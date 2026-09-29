WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        duration_sec - bpm AS duration_sec_minus_bpm
    FROM tracks
    WHERE (is_explicit = TRUE OR NOT ARRAY_CONTAINS(moods, 'focus'))
),
tagged AS (
    -- Add calculated columns
    SELECT *, CONCAT(title, '#', CAST(id AS STRING)) AS title_ref
    FROM recent
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM tagged s
    WHERE s.id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.source <> 'radio'
              AND TO_DATE(p.played_at) < DATE '2025-02-18'
        )
      AND UPPER(TRIM(s.title)) LIKE 'E%'
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.duration_sec_minus_bpm,
        s.title_ref,
        p.id AS play_id,
        p.source AS play_source,
        p.listener_id AS play_listener_id,
        p.is_skipped AS play_is_skipped
    FROM with_activity s
    LEFT JOIN plays p ON p.track_id = s.id
),
tagged_2 AS (
    -- Derive helper columns
    SELECT
        *,
        ARRAY_CONTAINS(moods, 'sad') AS has_sad,
        SUBSTRING(TRIM(title_ref), 1, 2) AS title_ref_prefix
    FROM with_children_plays
),
combined_details AS (
    -- Join plays
    SELECT s.*, p.device AS play_device, p.source AS play_source_2
    FROM tagged_2 s
    JOIN plays p ON p.id = s.play_id
    WHERE p.source = 'playlist'
)
SELECT
    play_id,
    play_source_2,
    GROUPING(play_id) AS is_total_play_id,
    PERCENTILE_APPROX(duration_sec, 0.5) AS median_duration_sec,
    ROUND(SUM(duration_sec_minus_bpm) / NULLIF(SUM(bpm), 0), 3) AS duration_sec_minus_bpm_to_bpm_ratio,
    COUNT(*) AS cnt
FROM combined_details
GROUP BY GROUPING SETS ((play_id, play_source_2), (play_id), (play_source_2), ())
ORDER BY play_id NULLS LAST, play_source_2 NULLS LAST
