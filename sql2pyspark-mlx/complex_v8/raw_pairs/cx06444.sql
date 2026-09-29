WITH cleaned_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        ADD_MONTHS(release_date, 12) AS release_date_plus_12m,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
    WHERE is_explicit = TRUE
      AND LENGTH(TRIM(title)) >= 4
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY YEAR(release_date_plus_12m)) AS rows_in_release_date_plus_12m_year,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_plus_12m) ORDER BY duration_sec ASC, id) AS duration_sec_quartile,
        ROUND(AVG(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_plus_12m) ORDER BY duration_sec ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_2_prior,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY artist_id ORDER BY release_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY YEAR(release_date_plus_12m))) AS distinct_artist_id_in_release_date_plus_12m_year,
        MIN(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_min_duration_sec,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_plus_12m) ORDER BY duration_sec ASC, id) AS duration_sec_decile
    FROM cleaned_tracks
    WINDOW w_release_date_plus_12m_year AS (PARTITION BY YEAR(release_date_plus_12m))
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(release_date, release_date_plus_12m) AS days_release_date_plus_12m_to_release_date,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        CASE WHEN first_duration_sec >= 312 THEN 'large' WHEN first_duration_sec >= 183 THEN 'medium' ELSE 'small' END AS first_duration_sec_tier
    FROM metrics
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        duration_sec,
        release_date,
        moods,
        release_date_plus_12m,
        duration_sec_quartile,
        distinct_artist_id_in_release_date_plus_12m_year,
        duration_sec_decile,
        days_release_date_plus_12m_to_release_date,
        days_since_release_date,
        first_duration_sec_tier,
        ROUND(duration_sec_decile / NULLIF(SUM(duration_sec_decile) OVER w_first_duration_sec_tier_dist, 0), 4) AS duration_sec_decile_share_of_first_duration_sec_,
        duration_sec_decile - LAG(duration_sec_decile) OVER (ORDER BY artist_id_min_duration_sec DESC, id) AS duration_sec_decile_change,
        LEAD(bpm, 2, 0) OVER (ORDER BY artist_id_min_duration_sec DESC, id) AS next2_bpm,
        RANK() OVER (ORDER BY artist_id_min_duration_sec DESC, id) AS artist_id_min_duration_sec_rank,
        ROUND(AVG(first_duration_sec) OVER (PARTITION BY first_duration_sec_tier, distinct_artist_id_in_release_date_plus_12m_year), 2) AS first_duration_sec_tier_distinct_artist_id_in_re,
        RANK() OVER (ORDER BY release_date ASC NULLS LAST, id) AS release_date_rank,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY release_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        ROUND(AVG(duration_sec) OVER (PARTITION BY first_duration_sec_tier, distinct_artist_id_in_release_date_plus_12m_year), 2) AS first_duration_sec_tier_distinct_artist_id_in_re_2
    FROM derived
    WINDOW w_first_duration_sec_tier_dist AS (PARTITION BY first_duration_sec_tier, distinct_artist_id_in_release_date_plus_12m_year)
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(last_known_release_date, 'yyyy-MM') AS last_known_release_date_ym,
        duration_sec_quartile,
        distinct_artist_id_in_release_date_plus_12m_year,
        COUNT(*) AS row_count,
        ROUND(STDDEV(first_duration_sec_tier_distinct_artist_id_in_re_2), 2) AS stddev_first_duration_sec_tier_distinct_artist_i,
        COUNT(DISTINCT first_duration_sec_tier) AS distinct_first_duration_sec_tiers,
        MIN(release_date_plus_12m) AS first_release_date_plus_12m,
        SUM(days_since_release_date) AS total_days_since_release_date,
        SUM(release_date_rank) AS total_release_date_rank
    FROM ranked
    WHERE ARRAY_CONTAINS(moods, 'chill')
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    last_known_release_date_ym,
    duration_sec_quartile,
    distinct_artist_id_in_release_date_plus_12m_year,
    row_count,
    stddev_first_duration_sec_tier_distinct_artist_i,
    distinct_first_duration_sec_tiers,
    first_release_date_plus_12m,
    total_days_since_release_date,
    COALESCE(total_release_date_rank, 0) AS total_release_date_rank_filled,
    LEAST(duration_sec_quartile, total_release_date_rank) AS least_duration_sec_quartile_total_release_date_r
FROM summary
ORDER BY stddev_first_duration_sec_tier_distinct_artist_i
