WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        TRUNC(release_date, 'MM') AS release_date_month_start,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 4
      AND bpm >= 96
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_artist_id_by_release_date_mo AS release_date_month_start_rank,
        SUM(bpm_per_duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm_per_duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_per_duration_sec_sum_6_prior,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY YEAR(release_date))) AS distinct_artist_id_in_release_date_year,
        LAG(bpm, 2) OVER (PARTITION BY artist_id ORDER BY release_date_month_start, id) AS prev2_bpm,
        COUNT(*) OVER w_release_date_year AS rows_in_release_date_year,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY artist_id ORDER BY release_date_month_start, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec
    FROM scoped
    WINDOW w_artist_id_by_release_date_mo AS (PARTITION BY artist_id ORDER BY release_date_month_start), w_release_date_year AS (PARTITION BY YEAR(release_date))
),
joined_artist AS (
    -- Join artists
    SELECT s.*, a.country AS artist_country
    FROM sequenced s
    LEFT JOIN artists a ON a.id = s.artist_id
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM joined_artist
    WHERE bpm_per_duration_sec_sum_6_prior > (SELECT AVG(bpm_per_duration_sec_sum_6_prior) FROM joined_artist)
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, duration_sec, distinct_artist_id_in_release_date_year
    FROM screened
) src
UNPIVOT (
    metric_value FOR metric_name IN (duration_sec, distinct_artist_id_in_release_date_year)
) unp
ORDER BY id, metric_name
