WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM tracks
    WHERE (LOWER(title) LIKE '%eo%' OR is_explicit = TRUE)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(5) OVER w_is_explicit_by_bpm AS bpm_quintile,
        LEAD(bpm, 2) OVER w_all_by_duration_sec_bucket AS next2_bpm,
        COUNT(*) OVER w_release_date_month AS rows_in_release_date_month,
        ROUND(duration_sec_bucket - AVG(duration_sec_bucket) OVER (PARTITION BY YEAR(release_date)), 2) AS duration_sec_bucket_vs_release_date_year_avg,
        COUNT(*) OVER (PARTITION BY YEAR(release_date)) AS rows_in_release_date_year,
        ROUND(duration_sec_bucket / NULLIF(SUM(duration_sec_bucket) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)), 0), 4) AS duration_sec_bucket_share_of_release_date_month,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        MIN(duration_sec_bucket) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_bucket_so_far,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY is_explicit ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        MAX(bpm) OVER (ORDER BY duration_sec_bucket, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS bpm_max_1_prior,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY YEAR(release_date))) AS distinct_artist_id_in_release_date_year,
        bpm - LAG(bpm) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec_bucket, id) AS bpm_change,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_2,
        MAX(duration_sec_bucket) OVER (PARTITION BY is_explicit ORDER BY bpm ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_bucket_max_4,
        LEAD(bpm) OVER w_all_by_duration_sec_bucket AS next_bpm
    FROM filtered
    WINDOW w_is_explicit_by_bpm AS (PARTITION BY is_explicit ORDER BY bpm ASC, id), w_all_by_duration_sec_bucket AS (ORDER BY duration_sec_bucket, id), w_release_date_month AS (PARTITION BY DATE_TRUNC('MONTH', release_date))
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY distinct_artist_id_in_release_date_year, bpm_quintile)) AS distinct_artist_id_in_distinct_artist_id_in_rele,
        LAST_VALUE(bpm_max_1_prior) IGNORE NULLS OVER (PARTITION BY distinct_artist_id_in_release_date_year ORDER BY rows_in_release_date_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm_max_1_prior,
        LEAD(release_date) OVER (ORDER BY second_bpm_2, id) AS next_release_date,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY distinct_artist_id_in_release_date_year ORDER BY rows_in_release_date_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        MAX(bpm) OVER (ORDER BY distinct_artist_id_in_release_date_year DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS bpm_max_3_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_artist_id_in_release_date_year ORDER BY rows_in_release_date_year DESC, id), 3) AS rows_in_release_date_year_cume_dist
    FROM ranked
    WINDOW w_distinct_artist_id_in_releas AS (PARTITION BY distinct_artist_id_in_release_date_year, bpm_quintile)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        artist_id,
        is_explicit,
        ROUND(AVG(rows_in_release_date_year_cume_dist), 2) AS avg_rows_in_release_date_year_cume_dist,
        ROUND(STDDEV(second_bpm_2), 2) AS stddev_second_bpm_2,
        DATEDIFF(MAX(next_release_date), MIN(next_release_date)) AS next_release_date_span_days
    FROM with_trends
    GROUP BY artist_id, is_explicit
)
SELECT
    artist_id,
    is_explicit,
    avg_rows_in_release_date_year_cume_dist,
    stddev_second_bpm_2,
    next_release_date_span_days
FROM rollup_base
WHERE avg_rows_in_release_date_year_cume_dist >= 0.1
ORDER BY stddev_second_bpm_2 DESC, artist_id, is_explicit
LIMIT 10
