WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        bpm - duration_sec AS bpm_minus_duration_sec,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 7
      AND duration_sec > 142
),
scored AS (
    -- Add calculated columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(title_ref), ' ', '_')) AS title_ref_slug
    FROM valid
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(bpm_minus_duration_sec / 100) * 100 AS bpm_minus_duration_sec_bucket,
        LENGTH(TRIM(title_ref)) AS title_ref_len
    FROM scored
),
latest_per_key AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        bpm_minus_duration_sec,
        title_ref,
        title_ref_slug,
        bpm_minus_duration_sec_bucket,
        title_ref_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm_minus_duration_sec DESC, id DESC) AS rn
        FROM derived
    ) ranked
    WHERE rn = 1
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.bpm_minus_duration_sec,
        s.title_ref,
        s.bpm_minus_duration_sec_bucket,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played
    FROM latest_per_key s
    LEFT JOIN plays p ON p.track_id = s.id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix,
        CASE WHEN play_is_skipped THEN 1 ELSE 0 END AS play_is_skipped_flag,
        ARRAY_CONTAINS(moods, 'workout') AS has_workout
    FROM line_level
)
SELECT
    id,
    artist_id,
    title,
    bpm,
    moods,
    bpm_minus_duration_sec,
    bpm_minus_duration_sec_bucket,
    play_id,
    play_played_at,
    play_is_skipped,
    play_ms_played,
    title_prefix,
    play_is_skipped_flag,
    has_workout
FROM tagged
ORDER BY play_played_at DESC, bpm
