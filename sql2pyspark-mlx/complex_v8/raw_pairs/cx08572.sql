WITH recent AS (
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
        CASE WHEN bpm >= 143 THEN 'large' WHEN bpm >= 94 THEN 'medium' ELSE 'small' END AS bpm_tier,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'party')
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.bpm_tier,
        s.days_since_release_date,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.listener_id AS play_listener_id,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at
    FROM recent s
    JOIN plays p ON p.track_id = s.id
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM detail_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.play_listener_id
              AND p.source IN ('radio', 'search', 'playlist')
              AND p.device NOT IN ('speaker', 'tv')
        )
),
annotated AS (
    -- Label and score rows
    SELECT *, CONCAT(title, '#', CAST(play_listener_id AS STRING)) AS title_ref
    FROM screened
    WHERE play_ms_played BETWEEN 44439 AND 245877
),
combined_details_play AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.moods,
        s.bpm_tier,
        s.play_id,
        s.play_ms_played,
        s.play_listener_id,
        s.title_ref,
        p.device AS play_device,
        p.source AS play_source,
        p.track_id AS play_track_id,
        l.interests AS listener_interests
    FROM annotated s
    LEFT JOIN plays p ON p.id = s.play_id
    JOIN listeners l ON l.id = s.play_listener_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        title,
        bpm,
        moods,
        bpm_tier,
        play_id,
        play_ms_played,
        play_device,
        play_source,
        listener_interests,
        ROUND(bpm - AVG(bpm) OVER w_play_id, 2) AS bpm_vs_play_id_avg,
        play_ms_played - LAG(play_ms_played) OVER (ORDER BY bpm DESC, id, play_id) AS play_ms_played_change
    FROM combined_details_play
    WINDOW w_play_id AS (PARTITION BY play_id)
)
-- Final projection
SELECT
    id,
    title,
    bpm,
    moods,
    bpm_tier,
    play_id,
    play_ms_played,
    play_device,
    play_source,
    listener_interests,
    bpm_vs_play_id_avg,
    play_ms_played_change
FROM metrics
