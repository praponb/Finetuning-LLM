WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref
    FROM tracks
    WHERE duration_sec < 353
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id, is_explicit), 2) AS artist_id_is_explicit_avg_duration_sec,
        RANK() OVER (PARTITION BY is_explicit ORDER BY duration_sec) AS duration_sec_rank,
        MAX(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS duration_sec_max_2,
        FIRST_VALUE(release_date) OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY duration_sec ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS duration_sec_share_of_artist_id_is_explicit,
        SUM(duration_sec) OVER w_artist_id_is_explicit AS artist_id_is_explicit_sum_duration_sec,
        LEAD(release_date) OVER (PARTITION BY is_explicit ORDER BY release_date ASC, id) AS next_release_date,
        ROW_NUMBER() OVER w_is_explicit_by_duration_sec_3 AS seq_no,
        NTILE(5) OVER (PARTITION BY is_explicit ORDER BY release_date ASC, id) AS release_date_quintile,
        LAST_VALUE(release_date) OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY duration_sec ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id, is_explicit), 2) AS duration_sec_vs_artist_id_is_explicit_avg,
        ROUND(AVG(duration_sec) OVER (PARTITION BY is_explicit ORDER BY release_date ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_3_prior,
        LAG(duration_sec) OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY duration_sec ASC NULLS LAST, id) AS prev_duration_sec,
        LEAD(duration_sec, 2, 0) OVER w_artist_id_release_date_year_2 AS next2_duration_sec
    FROM cleaned_tracks
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_is_explicit_by_duration_sec_3 AS (PARTITION BY is_explicit ORDER BY duration_sec, id), w_artist_id_release_date_year_2 AS (PARTITION BY artist_id, YEAR(release_date) ORDER BY duration_sec ASC NULLS LAST, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods,
        title_ref,
        duration_sec_max_2,
        first_release_date,
        seq_no,
        release_date_quintile,
        last_release_date,
        duration_sec_avg_3_prior,
        prev_duration_sec,
        next2_duration_sec,
        CASE WHEN release_date_quintile >= 8 THEN 'A' WHEN release_date_quintile >= 4 THEN 'B' ELSE 'C' END AS release_date_quintile_tier,
        ROUND(duration_sec_avg_3_prior / NULLIF(next2_duration_sec, 0), 3) AS duration_sec_avg_3_prior_per_next2_duration_sec,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM ranked
    WHERE artist_id_is_explicit_avg_duration_sec > 156.0
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        release_date_quintile - LAG(release_date_quintile) OVER w_is_explicit_by_duration_sec_4 AS release_date_quintile_change,
        ROW_NUMBER() OVER w_release_date_quintile_by_fir AS first_release_date_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY release_date_quintile ORDER BY duration_sec_avg_3_prior DESC, id), 3) AS duration_sec_avg_3_prior_percent_rank,
        LAST_VALUE(duration_sec_max_2) OVER (PARTITION BY release_date_quintile ORDER BY duration_sec_avg_3_prior DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec_max_2,
        ROUND(AVG(duration_sec_avg_3_prior) OVER (PARTITION BY release_date_quintile ORDER BY first_release_date DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_3_prior_avg_2_prior,
        duration_sec_avg_3_prior - LAG(duration_sec_avg_3_prior) OVER (PARTITION BY release_date_quintile ORDER BY duration_sec_avg_3_prior DESC, id) AS duration_sec_avg_3_prior_change,
        ROW_NUMBER() OVER w_is_explicit_by_duration_sec_4 AS rn
    FROM labeled
    WINDOW w_is_explicit_by_duration_sec_4 AS (PARTITION BY is_explicit ORDER BY duration_sec_avg_3_prior_per_next2_duration_sec DESC, id), w_release_date_quintile_by_fir AS (PARTITION BY release_date_quintile ORDER BY first_release_date DESC, id)
),
per_group AS (
    -- Summarise per group
    SELECT
        rn,
        DATE_FORMAT(first_release_date, 'yyyy-MM') AS first_release_date_ym,
        release_date_quintile_tier,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        ROUND(AVG(rn), 2) AS avg_rn,
        COUNT(*) AS cnt,
        COUNT(DISTINCT release_date_quintile_tier) AS distinct_release_date_quintile_tiers,
        PERCENTILE_APPROX(duration_sec_avg_3_prior_avg_2_prior, 0.5) AS median_duration_sec_avg_3_prior_avg_2_prior
    FROM metrics
    WHERE first_release_date BETWEEN DATE '2024-08-26' AND DATE '2025-04-02'
    GROUP BY rn, DATE_FORMAT(first_release_date, 'yyyy-MM'), release_date_quintile_tier
)
-- Final projection
SELECT
    rn,
    release_date_quintile_tier,
    distinct_artist_ids,
    avg_rn,
    cnt,
    distinct_release_date_quintile_tiers,
    median_duration_sec_avg_3_prior_avg_2_prior,
    LENGTH(TRIM(first_release_date_ym)) AS first_release_date_ym_len,
    ROUND(avg_rn * 0.25, 2) AS avg_rn_adj
FROM per_group
WHERE median_duration_sec_avg_3_prior_avg_2_prior <= 354.0
ORDER BY avg_rn DESC, distinct_artist_ids
