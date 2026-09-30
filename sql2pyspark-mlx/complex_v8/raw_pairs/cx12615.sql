WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        moods,
        DATE_TRUNC('YEAR', release_date) AS release_date_year,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref,
        DATE_ADD(release_date, 7) AS release_date_plus_7d
    FROM tracks
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.release_date_year,
        p.id AS play_id,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped
    FROM prepared s
    LEFT JOIN plays p ON p.track_id = s.id AND p.device = 'speaker'
),
combined_details AS (
    -- Join plays
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.release_date_year,
        s.play_id,
        s.play_device,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped_2
    FROM with_children s
    LEFT JOIN plays p ON p.id = s.play_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count_2
    FROM combined_details s
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm > 85
              AND t.release_date BETWEEN DATE '2024-06-03' AND DATE '2025-11-29'
        )
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        play_source,
        PERCENTILE_APPROX(play_count, 0.5) AS median_play_count,
        ROUND(AVG(play_count_2), 2) AS avg_play_count_2,
        ARRAY_SORT(COLLECT_SET(play_source)) AS play_source_list
    FROM without_matches
    GROUP BY play_source
    HAVING MAX(play_count) > 2
),
without_matches_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE avg_play_count_2 > (SELECT AVG(avg_play_count_2) FROM grouped)
)
-- Final projection
SELECT
    play_source,
    median_play_count,
    play_source_list,
    ARRAY_JOIN(ARRAY_SORT(play_source_list), ',') AS play_source_list_text
FROM without_matches_2
