WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE bpm BETWEEN 85 AND 166
      AND SIZE(moods) >= 2
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.moods,
        s.title_len,
        p.id AS play_id,
        p.source AS play_source,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped
    FROM base s
    JOIN plays p ON p.track_id = s.id
),
enriched AS (
    -- Attach play attributes
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.moods,
        s.play_id,
        s.play_ms_played,
        s.play_played_at,
        p.listener_id AS play_listener_id,
        p.source AS play_source_2,
        p.track_id AS play_track_id,
        l.country AS listener_country
    FROM line_level_plays s
    JOIN plays p ON p.id = s.play_id
    JOIN listeners l ON l.id = p.listener_id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ABS(bpm - 108) AS bpm_distance,
        LOWER(REGEXP_REPLACE(TRIM(play_source_2), ' ', '_')) AS play_source_2_slug,
        play_ms_played + bpm AS play_ms_played_plus_bpm
    FROM enriched
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        bpm,
        moods,
        play_id,
        play_ms_played,
        play_played_at,
        play_source_2,
        listener_country,
        play_ms_played_plus_bpm,
        QUARTER(play_played_at) AS play_played_at_quarter,
        play_played_at + INTERVAL 7 DAYS AS play_played_at_plus_7d
    FROM derived
    WHERE ARRAY_CONTAINS(moods, 'sad')
),
deduped AS (
    -- Keep the latest row per play_source_2
    SELECT
        id,
        bpm,
        moods,
        play_id,
        play_ms_played,
        play_played_at,
        play_source_2,
        listener_country,
        play_ms_played_plus_bpm,
        play_played_at_quarter,
        play_played_at_plus_7d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY play_source_2 ORDER BY play_played_at_quarter DESC, id DESC, play_id DESC) AS recency_rank
        FROM annotated
    ) ranked
    WHERE recency_rank = 1
)
SELECT *
FROM (
    SELECT play_played_at, listener_country, bpm
    FROM deduped
) src
PIVOT (
    SUM(bpm)
    FOR listener_country IN ('TH', 'JP', 'KR')
)
