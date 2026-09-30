WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods,
        duration_sec + bpm AS duration_sec_plus_bpm
    FROM tracks
    WHERE release_date >= DATE '2024-08-26'
),
enriched AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year,
        a.artist_name
    FROM scoped_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.is_skipped = TRUE
        )
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        duration_sec,
        artist_name,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        RANK() OVER (PARTITION BY is_explicit ORDER BY artist_debut_year, id) AS artist_debut_year_rank
    FROM screened
),
with_activity AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM metrics s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
        )
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.artist_name,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device,
        p.ms_played AS play_ms_played,
        p.source AS play_source
    FROM with_activity s
    JOIN plays p ON p.track_id = s.id
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        play_source,
        COUNT(*) AS cnt,
        SUM(duration_sec) FILTER (WHERE artist_name IS NOT NULL) AS duration_sec_artist_name_is_not_null,
        PERCENTILE_APPROX(duration_sec, 0.5) AS median_duration_sec,
        ROUND(100 * COUNT(CASE WHEN play_is_skipped = FALSE THEN 1 END) / COUNT(*), 1) AS pct_play_is_skipped_false,
        COUNT(DISTINCT play_source) AS distinct_play_sources,
        ARRAY_SORT(COLLECT_SET(play_source)) AS play_source_list
    FROM with_children_plays
    WHERE play_device IS NOT NULL
    GROUP BY artist_id, play_source
)
-- Final projection
SELECT
    artist_id,
    play_source,
    cnt,
    duration_sec_artist_name_is_not_null,
    median_duration_sec,
    pct_play_is_skipped_false,
    distinct_play_sources,
    UPPER(TRIM(play_source)) AS play_source_clean
FROM rollup_base
ORDER BY median_duration_sec
