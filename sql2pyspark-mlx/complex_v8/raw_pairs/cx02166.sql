WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE bpm > 96
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(bpm, 3, 0) OVER w_is_explicit_by_duration_sec AS prev3_bpm,
        COUNT(*) OVER (PARTITION BY artist_id, is_explicit) AS rows_in_artist_id_is_explicit,
        ROUND(AVG(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_3_prior,
        FIRST_VALUE(bpm) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        ROUND(AVG(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_2,
        MIN(bpm) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_min_bpm,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id, is_explicit), 2) AS duration_sec_vs_artist_id_is_explicit_avg,
        NTILE(5) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec DESC, id) AS duration_sec_quintile,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id, is_explicit), 2) AS bpm_vs_artist_id_is_explicit_avg,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit, artist_id), 0), 4) AS bpm_share_of_is_explicit_artist_id,
        ROW_NUMBER() OVER w_all_by_duration_sec AS seq_no
    FROM scoped
    WINDOW w_is_explicit_by_duration_sec AS (PARTITION BY is_explicit ORDER BY duration_sec DESC, id), w_all_by_duration_sec AS (ORDER BY duration_sec, id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        title_prefix,
        prev3_bpm,
        rows_in_artist_id_is_explicit,
        duration_sec_avg_3_prior,
        first_bpm,
        duration_sec_avg_2,
        is_explicit_artist_id_min_bpm,
        duration_sec_vs_artist_id_is_explicit_avg,
        duration_sec_quintile,
        bpm_vs_artist_id_is_explicit_avg,
        first_known_duration_sec,
        bpm_share_of_is_explicit_artist_id
    FROM with_windows
    WHERE seq_no <= 2
      AND first_bpm < 143
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        title_prefix,
        prev3_bpm,
        duration_sec_avg_3_prior,
        first_bpm,
        duration_sec_avg_2,
        is_explicit_artist_id_min_bpm,
        duration_sec_vs_artist_id_is_explicit_avg,
        duration_sec_quintile,
        bpm_vs_artist_id_is_explicit_avg,
        NTH_VALUE(first_bpm, 2) OVER (ORDER BY bpm_vs_artist_id_is_explicit_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_bpm,
        SUM(duration_sec_quintile) OVER w_artist_id_is_explicit AS artist_id_is_explicit_sum_duration_sec_quintile,
        NTH_VALUE(prev3_bpm, 2) OVER (ORDER BY bpm_vs_artist_id_is_explicit_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev3_bpm,
        ROUND(prev3_bpm - AVG(prev3_bpm) OVER (PARTITION BY artist_id, is_explicit), 2) AS prev3_bpm_vs_artist_id_is_explicit_avg,
        LAST_VALUE(prev3_bpm) OVER (ORDER BY bpm_vs_artist_id_is_explicit_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prev3_bpm,
        first_known_duration_sec - LAG(first_known_duration_sec) OVER (ORDER BY bpm_vs_artist_id_is_explicit_avg ASC, id) AS first_known_duration_sec_change,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id, is_explicit), 2) AS duration_sec_vs_artist_id_is_explicit_avg_2,
        ROUND(duration_sec_avg_2 - AVG(duration_sec_avg_2) OVER (PARTITION BY artist_id, is_explicit), 2) AS duration_sec_avg_2_vs_artist_id_is_explicit_avg
    FROM top_per_group
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit)
),
stats AS (
    -- Summarise per group
    SELECT
        artist_id,
        duration_sec_quintile,
        is_explicit,
        ROUND(SUM(duration_sec_vs_artist_id_is_explicit_avg_2) / NULLIF(SUM(duration_sec_avg_3_prior), 0), 3) AS duration_sec_vs_artist_id_is_explicit_avg_2_to_d,
        SUM(duration_sec_vs_artist_id_is_explicit_avg) AS total_duration_sec_vs_artist_id_is_explicit_avg,
        ROUND(AVG(first_bpm), 2) AS avg_first_bpm,
        SUM(artist_id_is_explicit_sum_duration_sec_quintile) AS total_artist_id_is_explicit_sum_duration_sec_qui,
        SUM(duration_sec_vs_artist_id_is_explicit_avg_2) AS total_duration_sec_vs_artist_id_is_explicit_avg_,
        PERCENTILE_APPROX(is_explicit_artist_id_min_bpm, 0.5) AS median_is_explicit_artist_id_min_bpm
    FROM metrics
    WHERE duration_sec_avg_2 <= 387.0
    GROUP BY 1, 2, 3
    HAVING MAX(first_known_duration_sec_change) > -336
)
-- Final projection
SELECT
    artist_id,
    duration_sec_quintile,
    is_explicit,
    duration_sec_vs_artist_id_is_explicit_avg_2_to_d,
    avg_first_bpm,
    total_artist_id_is_explicit_sum_duration_sec_qui,
    total_duration_sec_vs_artist_id_is_explicit_avg_,
    median_is_explicit_artist_id_min_bpm,
    CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
FROM stats
ORDER BY median_is_explicit_artist_id_min_bpm, is_explicit_flag
