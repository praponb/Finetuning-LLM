WITH prepared AS (
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
        ABS(duration_sec - 312) AS duration_sec_distance,
        DATE_ADD(release_date, 30) AS release_date_plus_30d
    FROM tracks
    WHERE SIZE(moods) >= 2
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM prepared s
    JOIN plays p ON p.track_id = s.id AND p.played_at >= TIMESTAMP '2025-01-25 10:00:00'
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country,
        p.device AS play_device_2,
        p.ms_played AS play_ms_played
    FROM exploded_detail s
    JOIN artists a ON a.id = s.artist_id
    JOIN plays p ON p.id = s.play_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year_2
    FROM with_details_artist s
    JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        play_id,
        play_played_at,
        play_device,
        artist_debut_year,
        artist_country,
        play_ms_played,
        artist_genre,
        artist_debut_year_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY play_ms_played), 3) AS play_ms_played_percent_rank,
        SIZE(COLLECT_SET(play_device) OVER w_artist_id_play_device_2) AS distinct_play_device_in_artist_id_play_device_2,
        ROUND(artist_debut_year_2 - AVG(artist_debut_year_2) OVER w_artist_id_play_device_2, 2) AS artist_debut_year_2_vs_artist_id_play_device_2_a,
        SUM(artist_debut_year) OVER w_artist_id_play_device_2 AS artist_id_play_device_2_sum_artist_debut_year
    FROM combined_details
    WINDOW w_artist_id_play_device_2 AS (PARTITION BY artist_id, play_device_2)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        title,
        bpm,
        play_id,
        play_played_at,
        artist_debut_year,
        artist_country,
        play_ms_played,
        artist_genre,
        play_ms_played_percent_rank,
        distinct_play_device_in_artist_id_play_device_2,
        artist_id_play_device_2_sum_artist_debut_year,
        CASE WHEN duration_sec >= 357 THEN 'A' WHEN duration_sec >= 213 THEN 'B' ELSE 'C' END AS duration_sec_tier
    FROM metrics
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    bpm,
    play_id,
    play_played_at,
    artist_country,
    play_ms_played,
    artist_genre,
    distinct_play_device_in_artist_id_play_device_2,
    duration_sec_tier,
    LOWER(REGEXP_REPLACE(TRIM(artist_country), ' ', '_')) AS artist_country_slug
FROM scored
