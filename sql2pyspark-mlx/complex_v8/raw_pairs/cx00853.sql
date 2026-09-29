WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE release_date >= DATE '2024-11-14'
      AND artist_id IS NOT NULL
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country,
        a.artist_name
    FROM recent_tracks s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        artist_debut_year,
        artist_country,
        LEAD(artist_debut_year, 2, 0) OVER (ORDER BY release_date DESC, id) AS next2_artist_debut_year,
        NTILE(3) OVER (PARTITION BY artist_id, artist_country ORDER BY release_date, id) AS release_date_tercile,
        LAG(release_date) OVER (PARTITION BY is_explicit ORDER BY artist_debut_year, id) AS prev_release_date,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY artist_country), 2) AS artist_debut_year_vs_artist_country_avg,
        DATEDIFF(release_date, LAG(release_date) OVER w_is_explicit_by_artist_debut) AS days_since_prev_release_date,
        MIN(artist_debut_year) OVER (PARTITION BY YEAR(release_date), artist_id) AS release_date_year_artist_id_min_artist_debut_yea,
        MIN(artist_debut_year) OVER (PARTITION BY is_explicit ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_artist_debut_year_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY artist_debut_year, id), 3) AS artist_debut_year_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY artist_debut_year, id), 3) AS artist_debut_year_cume_dist_2,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_country), 2) AS artist_country_avg_artist_debut_year,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER (PARTITION BY YEAR(release_date), artist_id), 0), 4) AS artist_debut_year_share_of_release_date_year_art,
        RANK() OVER (PARTITION BY is_explicit ORDER BY artist_debut_year) AS artist_debut_year_rank,
        COUNT(*) OVER (PARTITION BY YEAR(release_date), artist_id) AS rows_in_release_date_year_artist_id,
        SUM(artist_debut_year) OVER (PARTITION BY YEAR(release_date), artist_id) AS release_date_year_artist_id_sum_artist_debut_yea
    FROM combined_details_artist
    WINDOW w_is_explicit_by_artist_debut AS (PARTITION BY is_explicit ORDER BY artist_debut_year, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        artist_country,
        MIN(artist_debut_year_share_of_release_date_year_art) AS min_artist_debut_year_share_of_release_date_year,
        COUNT(DISTINCT artist_country) AS distinct_artist_countrys,
        COUNT(*) AS n_rows
    FROM windowed
    WHERE release_date >= DATE '2024-08-26'
    GROUP BY 1, 2
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        release_date_month,
        artist_country,
        min_artist_debut_year_share_of_release_date_year,
        ROUND(PERCENT_RANK() OVER w_n_rows_artist_country_by_n_r, 3) AS n_rows_percent_rank,
        RANK() OVER (PARTITION BY n_rows, artist_country ORDER BY n_rows ASC, release_date_month, artist_country) AS n_rows_rank,
        COUNT(*) OVER (PARTITION BY distinct_artist_countrys, artist_country) AS rows_in_distinct_artist_countrys_artist_country,
        ROUND(AVG(distinct_artist_countrys) OVER w_distinct_artist_countrys_art, 2) AS distinct_artist_countrys_artist_country_avg_dist
    FROM grouped
    WINDOW w_n_rows_artist_country_by_n_r AS (PARTITION BY n_rows, artist_country ORDER BY n_rows ASC, release_date_month, artist_country), w_distinct_artist_countrys_art AS (PARTITION BY distinct_artist_countrys, artist_country)
)
SELECT
    release_date_month,
    artist_country,
    n_rows_percent_rank,
    rows_in_distinct_artist_countrys_artist_country,
    distinct_artist_countrys_artist_country_avg_dist,
    LOWER(REGEXP_REPLACE(TRIM(artist_country), ' ', '_')) AS artist_country_slug,
    n_rows_percent_rank - n_rows_rank AS n_rows_percent_rank_minus_n_rows_rank
FROM sequenced
ORDER BY n_rows_percent_rank_minus_n_rows_rank DESC, rows_in_distinct_artist_countrys_artist_country DESC
