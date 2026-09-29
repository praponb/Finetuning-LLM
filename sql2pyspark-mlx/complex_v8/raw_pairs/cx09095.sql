WITH base_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CASE WHEN debut_year >= 2018 THEN 'large' WHEN debut_year >= 1994 THEN 'medium' ELSE 'small' END AS debut_year_tier
    FROM artists
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        debut_year_tier,
        ROUND(PERCENT_RANK() OVER (PARTITION BY debut_year_tier ORDER BY debut_year), 3) AS debut_year_percent_rank,
        SUM(debut_year) OVER (PARTITION BY debut_year_tier) AS debut_year_tier_sum_debut_year,
        COUNT(*) OVER (PARTITION BY debut_year_tier ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM base_artists
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count_2,
        (SELECT MIN(t.bpm) FROM tracks t WHERE t.artist_id = s.id) AS min_track_bpm
    FROM metrics s
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count_3
    FROM with_stats s
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.bpm <= 142
              AND t.duration_sec <= 312
        )
      AND s.debut_year BETWEEN 1996 AND 2020
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(t.bpm), 2) FROM tracks t WHERE t.artist_id = s.id) AS avg_track_bpm
    FROM qualified s
)
-- Final projection
SELECT
    id,
    artist_name,
    genre,
    country,
    debut_year,
    is_verified,
    debut_year_tier,
    debut_year_tier_sum_debut_year,
    running_count,
    min_track_bpm,
    track_count_3
FROM summarized_rows
WHERE track_count_2 BETWEEN 2 AND 6
ORDER BY country NULLS LAST
