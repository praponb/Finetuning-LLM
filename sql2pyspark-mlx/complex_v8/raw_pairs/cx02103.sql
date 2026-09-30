WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        moods
    FROM tracks
    WHERE artist_id >= 9
),
with_details AS (
    -- Join artists
    SELECT s.*, a.debut_year AS artist_debut_year
    FROM base s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year >= 1996
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(artist_debut_year) OVER w_artist_id, 2) AS artist_id_avg_artist_debut_year,
        MIN(artist_debut_year) OVER (PARTITION BY artist_id) AS artist_id_min_artist_debut_year,
        ROUND(CUME_DIST() OVER w_artist_id_by_artist_debut_ye, 3) AS artist_debut_year_cume_dist,
        LEAD(release_date, 2) OVER w_all_by_artist_debut_year AS next2_release_date,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER (PARTITION BY artist_id), 0), 4) AS artist_debut_year_share_of_artist_id,
        LAG(release_date, 3) OVER (PARTITION BY YEAR(release_date) ORDER BY release_date, id) AS prev3_release_date,
        MIN(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_artist_debut_year_so_far,
        SUM(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY artist_debut_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_artist_debut_year,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER w_artist_id, 2) AS artist_debut_year_vs_artist_id_avg,
        ROUND(PERCENT_RANK() OVER w_release_date_year_by_release, 3) AS release_date_percent_rank
    FROM with_details
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_artist_id_by_artist_debut_ye AS (PARTITION BY artist_id ORDER BY artist_debut_year ASC), w_all_by_artist_debut_year AS (ORDER BY artist_debut_year NULLS LAST, id), w_release_date_year_by_release AS (PARTITION BY YEAR(release_date) ORDER BY release_date, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        YEAR(next2_release_date) AS next2_release_date_year,
        YEAR(prev3_release_date) AS prev3_release_date_year,
        PERCENTILE_APPROX(running_artist_debut_year, 0.5) AS median_running_artist_debut_year,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list
    FROM with_trends
    GROUP BY DATE_TRUNC('MONTH', release_date), YEAR(next2_release_date), YEAR(prev3_release_date)
    HAVING AVG(release_date_percent_rank) > 0.4
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY n_rows ASC), 3) AS n_rows_cume_dist,
        SUM(prev3_release_date_year) OVER (ORDER BY n_rows ASC, release_date_month, next2_release_date_year, prev3_release_date_year ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS prev3_release_date_year_sum_4
    FROM summary
)
-- Final projection
SELECT
    release_date_month,
    prev3_release_date_year,
    median_running_artist_debut_year,
    n_rows,
    title_list,
    n_rows_cume_dist,
    prev3_release_date_year_sum_4,
    DATEDIFF(DATE '2025-12-31', release_date_month) AS days_since_release_date_month,
    TO_DATE(release_date_month) AS release_date_month_date
FROM with_trends_2
ORDER BY median_running_artist_debut_year NULLS LAST
