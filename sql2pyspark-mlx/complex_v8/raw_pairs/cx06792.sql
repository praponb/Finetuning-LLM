WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        moods,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE (SIZE(moods) >= 2 OR LOWER(title) LIKE '% l%')
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.moods,
        s.days_since_release_date,
        s.bpm_bucket,
        a.country AS artist_country
    FROM valid s
    JOIN artists a ON a.id = s.artist_id
),
summary AS (
    -- Summarise per group
    SELECT
        artist_country,
        SUM(bpm_bucket) AS total_bpm_bucket,
        ROUND(SUM(bpm_bucket) / NULLIF(SUM(days_since_release_date), 0), 3) AS bpm_bucket_to_days_since_release_date_ratio,
        ROUND(SUM(days_since_release_date) / NULLIF(SUM(bpm_bucket), 0), 3) AS days_since_release_date_to_bpm_bucket_ratio,
        MIN(release_date) AS first_release_date
    FROM with_details_artist
    WHERE days_since_release_date < 595
    GROUP BY artist_country
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        artist_country,
        bpm_bucket_to_days_since_release_date_ratio,
        SIZE(COLLECT_SET(artist_country) OVER w_first_release_date_year) AS distinct_artist_country_in_first_release_date_ye,
        NTILE(5) OVER w_first_release_date_year_by_b AS bpm_bucket_to_days_since_release_date_ratio_quin,
        LAG(total_bpm_bucket, 2) OVER (PARTITION BY YEAR(first_release_date) ORDER BY bpm_bucket_to_days_since_release_date_ratio DESC, artist_country) AS prev2_total_bpm_bucket,
        RANK() OVER (PARTITION BY YEAR(first_release_date) ORDER BY bpm_bucket_to_days_since_release_date_ratio DESC) AS bpm_bucket_to_days_since_release_date_ratio_rank,
        RANK() OVER (PARTITION BY YEAR(first_release_date) ORDER BY bpm_bucket_to_days_since_release_date_ratio DESC, artist_country) AS bpm_bucket_to_days_since_release_date_ratio_rank_2
    FROM summary
    WINDOW w_first_release_date_year AS (PARTITION BY YEAR(first_release_date)), w_first_release_date_year_by_b AS (PARTITION BY YEAR(first_release_date) ORDER BY bpm_bucket_to_days_since_release_date_ratio DESC, artist_country)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT bpm_bucket_to_days_since_release_date_ratio_quin, bpm_bucket_to_days_since_release_date_ratio_rank_2, artist_country, distinct_artist_country_in_first_release_date_ye
    FROM sequenced
) src
PIVOT (
    AVG(distinct_artist_country_in_first_release_date_ye)
    FOR artist_country IN ('BR', 'US')
)
