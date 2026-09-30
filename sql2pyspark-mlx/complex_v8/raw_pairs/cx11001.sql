WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        CASE WHEN device IN ('car', 'tv') THEN 'group_a' WHEN device = 'speaker' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE listener_id IS NOT NULL
      AND is_skipped = FALSE
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        ms_played,
        device,
        device_group,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        LOWER(TRIM(device_group)) AS device_group_clean,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour
    FROM prepared
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS max_play_ms_played,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS min_play_ms_played,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM derived s
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.track_id,
        s.ms_played,
        s.device,
        s.device_group,
        s.played_at_label,
        s.device_group_clean,
        s.min_play_ms_played,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title,
        t.bpm AS track_bpm,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM summarized_rows s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
combined_details_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.track_id,
        s.ms_played,
        s.device,
        s.played_at_label,
        s.device_group_clean,
        s.min_play_ms_played,
        s.track_duration_sec,
        s.track_title,
        s.track_bpm,
        s.artist_debut_year,
        s.artist_country,
        t.title AS track_title_2,
        t.release_date AS track_release_date,
        t.moods AS track_moods,
        a.genre AS artist_genre,
        a.country AS artist_country_2
    FROM combined_details s
    LEFT JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
play_stats_2 AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MIN(played_at) AS first_played_at,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played,
        SUM(ms_played) AS total_play_ms_played
    FROM plays
    GROUP BY track_id
),
summarized_rows_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at,
        p.avg_play_ms_played,
        p.total_play_ms_played
    FROM combined_details_track s
    LEFT JOIN play_stats_2 p ON p.track_id = s.track_id
)
SELECT
    id,
    ms_played,
    device,
    played_at_label,
    min_play_ms_played,
    track_bpm,
    artist_debut_year,
    track_release_date,
    track_moods,
    first_played_at,
    avg_play_ms_played,
    total_play_ms_played,
    LOWER(TRIM(device)) AS device_clean,
    DATEDIFF(DATE '2025-12-31', track_release_date) AS days_since_track_release_date
FROM summarized_rows_2
ORDER BY id DESC, played_at_label DESC NULLS FIRST
