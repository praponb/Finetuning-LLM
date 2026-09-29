WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 7
),
combined_details AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.moods,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM base s
    JOIN artists a ON a.id = s.artist_id AND a.genre IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(release_date) OVER w_all_by_duration_sec AS next_release_date,
        COUNT(*) OVER w_release_date_year AS rows_in_release_date_year,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec), 3) AS duration_sec_percent_rank,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY YEAR(release_date)), 2) AS duration_sec_vs_release_date_year_avg,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY artist_country ORDER BY release_date, id)) AS days_since_prev_release_date,
        MIN(duration_sec) OVER (PARTITION BY artist_country ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far,
        MAX(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_max_6_prior,
        MAX(duration_sec) OVER (PARTITION BY artist_country, artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_so_far
    FROM combined_details
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec, id), w_release_date_year AS (PARTITION BY YEAR(release_date))
)
-- Final projection
SELECT
    id,
    duration_sec,
    moods,
    artist_country,
    artist_is_verified,
    next_release_date,
    rows_in_release_date_year,
    duration_sec_percent_rank,
    min_duration_sec_so_far,
    duration_sec_max_6_prior,
    max_duration_sec_so_far
FROM with_trends
WHERE max_duration_sec_so_far <= 3378
ORDER BY artist_country DESC, max_duration_sec_so_far DESC
