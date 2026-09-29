WITH scoped_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        debut_year
    FROM artists
    WHERE country = 'BR'
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM scoped_artists s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.bpm >= 94
              AND LENGTH(TRIM(t.title)) >= 6
        )
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS first_track_release_date,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS last_track_release_date
    FROM without_matches s
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_last_track_release_date_year AS rows_in_last_track_release_date_year,
        DATEDIFF(first_track_release_date, LAG(first_track_release_date) OVER (PARTITION BY MONTH(last_track_release_date) ORDER BY first_track_release_date, id)) AS days_since_prev_first_track_release_date,
        ROUND(debut_year / NULLIF(SUM(debut_year) OVER w_last_track_release_date_year, 0), 4) AS debut_year_share_of_last_track_release_date_year,
        MAX(debut_year) OVER (PARTITION BY MONTH(first_track_release_date) ORDER BY debut_year DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS debut_year_max_2_prior
    FROM profiled
    WINDOW w_last_track_release_date_year AS (PARTITION BY YEAR(last_track_release_date))
),
genre_stats AS (
    -- Median debut_year per genre
    SELECT genre, PERCENTILE_APPROX(debut_year, 0.5) AS median_debut_year
    FROM metrics
    GROUP BY genre
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_debut_year,
        ROUND(s.debut_year - g.median_debut_year, 2) AS debut_year_minus_median
    FROM metrics s
    JOIN genre_stats g ON g.genre = s.genre
    WHERE s.debut_year >= g.median_debut_year
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY genre ORDER BY last_track_release_date DESC, id) AS last_track_release_date_tercile,
        ROUND(debut_year / NULLIF(SUM(debut_year) OVER (PARTITION BY MONTH(last_track_release_date), MONTH(first_track_release_date)), 0), 4) AS debut_year_share_of_last_track_release_date_mont,
        SIZE(COLLECT_SET(genre) OVER w_last_track_release_date_mont_2) AS distinct_genre_in_last_track_release_date_month_,
        ROUND(AVG(debut_year_minus_median) OVER (PARTITION BY MONTH(last_track_release_date), MONTH(first_track_release_date)), 2) AS last_track_release_date_month_first_track_releas,
        ROUND(AVG(debut_year_max_2_prior) OVER (PARTITION BY MONTH(last_track_release_date), MONTH(first_track_release_date)), 2) AS last_track_release_date_month_first_track_releas_2
    FROM outliers
    WINDOW w_last_track_release_date_mont_2 AS (PARTITION BY MONTH(last_track_release_date), MONTH(first_track_release_date))
)
-- Final projection
SELECT
    id,
    artist_name,
    first_track_release_date,
    days_since_prev_first_track_release_date,
    debut_year_max_2_prior,
    median_debut_year,
    last_track_release_date_tercile,
    debut_year_share_of_last_track_release_date_mont,
    last_track_release_date_month_first_track_releas,
    CASE WHEN genre IN ('Classical', 'HipHop') THEN 'group_a' WHEN genre = 'Indie' THEN 'group_b' ELSE 'other' END AS genre_group
FROM windowed
ORDER BY debut_year_share_of_last_track_release_date_mont
