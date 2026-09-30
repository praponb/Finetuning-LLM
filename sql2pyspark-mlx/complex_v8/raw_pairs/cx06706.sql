WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        CASE WHEN bpm >= 155 THEN 'HIGH' WHEN bpm >= 96 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier
    FROM tracks
    WHERE duration_sec <= 352
      AND LOWER(title) LIKE '%ea%'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        bpm_tier,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id, MONTH(release_date)), 0), 4) AS bpm_share_of_artist_id_release_date_month,
        COUNT(*) OVER (PARTITION BY artist_id, MONTH(release_date)) AS rows_in_artist_id_release_date_month,
        ROUND(bpm - AVG(bpm) OVER w_artist_id_release_date_month, 2) AS bpm_vs_artist_id_release_date_month_avg,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, MONTH(release_date)), 2) AS artist_id_release_date_month_avg_bpm,
        MAX(bpm) OVER (PARTITION BY artist_id, MONTH(release_date)) AS artist_id_release_date_month_max_bpm,
        ROUND(CUME_DIST() OVER (ORDER BY release_date ASC), 3) AS release_date_cume_dist,
        FIRST_VALUE(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        MAX(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit ORDER BY release_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_max_6_prior,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit ORDER BY release_date, id)) AS days_since_prev_release_date,
        SUM(bpm) OVER w_artist_id_release_date_month AS artist_id_release_date_month_sum_bpm,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        LEAD(release_date) OVER (ORDER BY release_date ASC, id) AS next_release_date,
        LEAD(bpm, 2) OVER w_all_by_release_date AS next2_bpm,
        ROW_NUMBER() OVER w_all_by_release_date AS rn,
        RANK() OVER (ORDER BY release_date ASC) AS release_date_rank,
        SIZE(COLLECT_SET(bpm_tier) OVER (PARTITION BY artist_id, MONTH(release_date))) AS distinct_bpm_tier_in_artist_id_release_date_mont
    FROM cleaned
    WINDOW w_artist_id_release_date_month AS (PARTITION BY artist_id, MONTH(release_date)), w_all_by_release_date AS (ORDER BY release_date ASC, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE rn = 1
      AND artist_id_release_date_month_sum_bpm >= 582
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY rows_in_artist_id_release_date_month DESC NULLS FIRST, id) AS rows_in_artist_id_release_date_month_decile,
        LEAD(release_date_rank) OVER w_all_by_rows_in_artist_id_rel AS next_release_date_rank,
        ROW_NUMBER() OVER w_is_explicit_release_date_ran AS seq_no,
        NTH_VALUE(bpm_max_6_prior, 2) OVER (ORDER BY rows_in_artist_id_release_date_month DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_max_6_prior,
        SIZE(COLLECT_SET(bpm_tier) OVER (PARTITION BY distinct_bpm_tier_in_artist_id_release_date_mont)) AS distinct_bpm_tier_in_distinct_bpm_tier_in_artist,
        LEAD(next_release_date, 2) OVER (ORDER BY rows_in_artist_id_release_date_month DESC NULLS FIRST, id) AS next2_next_release_date
    FROM first_rows
    WINDOW w_all_by_rows_in_artist_id_rel AS (ORDER BY rows_in_artist_id_release_date_month DESC NULLS FIRST, id), w_is_explicit_release_date_ran AS (PARTITION BY is_explicit, release_date_rank ORDER BY days_since_prev_release_date ASC, id)
)
SELECT
    id,
    title,
    is_explicit,
    bpm,
    bpm_tier,
    rows_in_artist_id_release_date_month,
    artist_id_release_date_month_max_bpm,
    release_date_cume_dist,
    bpm_max_6_prior,
    artist_id_release_date_month_sum_bpm,
    first_known_bpm,
    next_release_date,
    next2_bpm,
    rn,
    release_date_rank,
    distinct_bpm_tier_in_artist_id_release_date_mont,
    next_release_date_rank,
    seq_no,
    distinct_bpm_tier_in_distinct_bpm_tier_in_artist,
    next2_next_release_date,
    SUBSTRING(TRIM(title), 1, 3) AS title_prefix
FROM ranked
ORDER BY first_known_bpm, rn
