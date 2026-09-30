WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        YEAR(release_date) AS release_date_year,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
),
rollup_base AS (
    -- Group-level totals
    SELECT
        is_explicit,
        YEAR(release_date) AS release_date_year_2,
        artist_id,
        PERCENTILE_APPROX(bpm_bucket, 0.5) AS median_bpm_bucket,
        PERCENTILE_APPROX(title_len, 0.5) AS median_title_len,
        SUM(release_date_year) AS total_release_date_year
    FROM base_tracks
    GROUP BY is_explicit, YEAR(release_date), artist_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.is_explicit,
        s.release_date_year_2,
        s.artist_id,
        s.median_bpm_bucket,
        s.median_title_len,
        a.genre AS artist_genre
    FROM rollup_base s
    LEFT JOIN artists a ON a.id = s.artist_id
),
annotated AS (
    -- Label and score rows
    SELECT
        is_explicit,
        release_date_year_2,
        artist_id,
        median_bpm_bucket,
        median_title_len,
        artist_genre,
        COALESCE(median_title_len, 0) AS median_title_len_filled,
        CASE WHEN release_date_year_2 >= 2025 THEN 'HIGH' WHEN release_date_year_2 >= 2024 THEN 'MEDIUM' ELSE 'LOW' END AS release_date_year_2_tier
    FROM with_details
    WHERE release_date_year_2 < 2026
),
filtered AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.title IS NOT NULL
              AND t.duration_sec >= 213
        )
      AND s.release_date_year_2_tier = 'LOW'
),
line_level_tracks AS (
    -- One row per track
    SELECT
        s.is_explicit,
        s.release_date_year_2,
        s.artist_id,
        s.median_title_len,
        s.artist_genre,
        s.median_title_len_filled,
        s.release_date_year_2_tier,
        t.id AS track_id,
        t.duration_sec AS track_duration_sec,
        t.moods AS track_moods,
        t.title AS track_title,
        t.release_date AS track_release_date
    FROM filtered s
    JOIN tracks t ON t.artist_id = s.artist_id
),
release_date_year_2_tier_stats AS (
    -- Max median_title_len_filled per release_date_year_2_tier
    SELECT release_date_year_2_tier, MAX(median_title_len_filled) AS max_median_title_len_filled
    FROM line_level_tracks
    GROUP BY release_date_year_2_tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_median_title_len_filled,
        ROUND(s.median_title_len_filled - g.max_median_title_len_filled, 2) AS median_title_len_filled_minus_max
    FROM line_level_tracks s
    JOIN release_date_year_2_tier_stats g ON g.release_date_year_2_tier = s.release_date_year_2_tier
    WHERE s.median_title_len_filled = g.max_median_title_len_filled
)
SELECT
    is_explicit,
    median_title_len,
    MIN(track_release_date) AS first_track_release_date,
    COUNT(DISTINCT artist_id) AS distinct_artist_ids,
    MAX(median_title_len) AS max_median_title_len
FROM vs_group
GROUP BY GROUPING SETS ((is_explicit, median_title_len), (is_explicit), (median_title_len), ())
ORDER BY is_explicit NULLS LAST, median_title_len NULLS LAST
