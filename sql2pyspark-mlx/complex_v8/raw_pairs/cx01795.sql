WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        DATE_ADD(release_date, 90) AS release_date_plus_90d,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE bpm >= 96
      AND artist_id <= 12
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT s.*, a.country AS artist_country, a.is_verified AS artist_is_verified
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(duration_sec) OVER (ORDER BY bpm DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_1_prior,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_plus_90d)), 2) AS duration_sec_vs_release_date_plus_90d_month_avg,
        RANK() OVER (PARTITION BY artist_id, artist_country ORDER BY release_date_plus_90d, id) AS release_date_plus_90d_rank,
        ROUND(bpm - AVG(bpm) OVER w_artist_id, 2) AS bpm_vs_artist_id_avg,
        SUM(bpm) OVER w_release_date_plus_90d_month AS release_date_plus_90d_month_sum_bpm,
        NTILE(3) OVER w_all_by_bpm AS bpm_tercile,
        DATEDIFF(release_date_plus_90d, LAG(release_date_plus_90d) OVER w_all_by_duration_sec) AS days_since_prev_release_date_plus_90d,
        MIN(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_plus_90d)) AS release_date_plus_90d_month_min_duration_sec,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec DESC, id), 3) AS duration_sec_cume_dist
    FROM joined_artist
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_release_date_plus_90d_month AS (PARTITION BY DATE_TRUNC('MONTH', release_date_plus_90d)), w_all_by_bpm AS (ORDER BY bpm DESC, id), w_all_by_duration_sec AS (ORDER BY duration_sec NULLS LAST, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', release_date_plus_90d) AS days_since_release_date_plus_90d
    FROM ranked
    WHERE NOT artist_is_verified
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        duration_sec,
        is_explicit,
        moods,
        release_date_plus_90d,
        release_date_label,
        artist_is_verified,
        duration_sec_avg_1_prior,
        duration_sec_vs_artist_id_avg,
        release_date_plus_90d_rank,
        bpm_vs_artist_id_avg,
        days_since_prev_release_date_plus_90d,
        days_since_release_date_plus_90d,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY release_date_plus_90d_rank), 2) AS duration_sec_vs_release_date_plus_90d_rank_avg,
        DENSE_RANK() OVER (PARTITION BY artist_country, release_date_plus_90d_rank ORDER BY release_date_plus_90d_month_min_duration_sec) AS release_date_plus_90d_month_min_duration_sec_den,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec_cume_dist DESC, id), 3) AS duration_sec_cume_dist_percent_rank,
        RANK() OVER (ORDER BY duration_sec_cume_dist DESC) AS duration_sec_cume_dist_rank,
        COUNT(*) OVER (PARTITION BY artist_country, release_date_plus_90d_rank ORDER BY release_date_plus_90d_month_min_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        COUNT(*) OVER w_release_date_plus_90d_rank AS rows_in_release_date_plus_90d_rank,
        ROUND(AVG(duration_sec_cume_dist) OVER w_release_date_plus_90d_rank, 2) AS release_date_plus_90d_rank_avg_duration_sec_cume,
        LEAD(duration_sec) OVER (PARTITION BY artist_country, release_date_plus_90d_rank ORDER BY release_date_plus_90d_month_min_duration_sec, id) AS next_duration_sec
    FROM enriched
    WINDOW w_release_date_plus_90d_rank AS (PARTITION BY release_date_plus_90d_rank)
),
totals AS (
    -- Summarise per group
    SELECT
        is_explicit,
        duration_sec_cume_dist_rank,
        release_date_plus_90d_month_min_duration_sec_den,
        PERCENTILE_APPROX(release_date_plus_90d_rank_avg_duration_sec_cume, 0.5) AS median_release_date_plus_90d_rank_avg_duration_s,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE NOT is_explicit) AS n_not_is_explicit,
        SUM(duration_sec) AS total_duration_sec,
        ROUND(SUM(duration_sec_vs_artist_id_avg), 2) AS total_duration_sec_vs_artist_id_avg
    FROM ranked_2
    WHERE duration_sec_vs_release_date_plus_90d_rank_avg <= 168.0
    GROUP BY 1, 2, 3
    HAVING MAX(duration_sec_cume_dist_percent_rank) > 0.1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        is_explicit,
        duration_sec_cume_dist_rank,
        release_date_plus_90d_month_min_duration_sec_den,
        median_release_date_plus_90d_rank_avg_duration_s,
        distinct_ids,
        n_not_is_explicit,
        total_duration_sec,
        LAST_VALUE(total_duration_sec) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY n_not_is_explicit, is_explicit, duration_sec_cume_dist_rank, release_date_plus_90d_month_min_duration_sec_den ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_total_duration_sec,
        MAX(distinct_ids) OVER (PARTITION BY is_explicit ORDER BY n_not_is_explicit, is_explicit, duration_sec_cume_dist_rank, release_date_plus_90d_month_min_duration_sec_den ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_distinct_ids_so_far,
        MAX(n_not_is_explicit) OVER (PARTITION BY is_explicit ORDER BY n_not_is_explicit, is_explicit, duration_sec_cume_dist_rank, release_date_plus_90d_month_min_duration_sec_den ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_n_not_is_explicit_so_far
    FROM totals
)
-- Final projection
SELECT
    is_explicit,
    duration_sec_cume_dist_rank,
    median_release_date_plus_90d_rank_avg_duration_s,
    distinct_ids,
    total_duration_sec,
    last_known_total_duration_sec,
    max_distinct_ids_so_far,
    max_n_not_is_explicit_so_far,
    GREATEST(max_distinct_ids_so_far, distinct_ids) AS greatest_max_distinct_ids_so_far_distinct_ids,
    ROUND(max_distinct_ids_so_far / NULLIF(total_duration_sec, 0), 3) AS max_distinct_ids_so_far_per_total_duration_sec
FROM sequenced
ORDER BY max_distinct_ids_so_far DESC
