WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CASE WHEN bpm >= 127 THEN 'HIGH' WHEN bpm >= 100 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier
    FROM tracks
    WHERE release_date >= DATE '2024-05-12'
),
summary AS (
    -- Summarise per group
    SELECT
        is_explicit,
        CASE WHEN duration_sec >= 392 THEN 'large' WHEN duration_sec >= 183 THEN 'medium' ELSE 'small' END AS duration_sec_tier,
        DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days,
        SUM(duration_sec) AS total_duration_sec,
        SUM(CASE WHEN is_explicit = TRUE THEN 1 ELSE 0 END) AS n_is_explicit_true,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        ROUND(SUM(duration_sec) / NULLIF(SUM(bpm), 0), 3) AS duration_sec_to_bpm_ratio
    FROM valid
    GROUP BY is_explicit, CASE WHEN duration_sec >= 392 THEN 'large' WHEN duration_sec >= 183 THEN 'medium' ELSE 'small' END
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(n_is_explicit_true - 4) AS n_is_explicit_true_distance,
        CONCAT(duration_sec_tier, '#', CAST(distinct_artist_ids AS STRING)) AS duration_sec_tier_ref,
        n_is_explicit_true + total_duration_sec AS n_is_explicit_true_plus_total_duration_sec
    FROM summary
),
tagged AS (
    -- Add calculated columns
    SELECT
        is_explicit,
        duration_sec_tier,
        release_date_span_days,
        n_is_explicit_true,
        distinct_artist_ids,
        duration_sec_to_bpm_ratio,
        n_is_explicit_true_distance,
        n_is_explicit_true_plus_total_duration_sec,
        LEAST(total_duration_sec, n_is_explicit_true) AS least_total_duration_sec_n_is_explicit_true
    FROM labeled
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(n_is_explicit_true_distance) OVER w_distinct_artist_ids, 2) AS distinct_artist_ids_avg_n_is_explicit_true_dista,
        ROUND(distinct_artist_ids / NULLIF(SUM(distinct_artist_ids) OVER (PARTITION BY distinct_artist_ids), 0), 4) AS distinct_artist_ids_share_of_distinct_artist_ids,
        LAG(n_is_explicit_true, 2) OVER (PARTITION BY n_is_explicit_true, n_is_explicit_true_distance ORDER BY n_is_explicit_true_plus_total_duration_sec DESC, is_explicit, duration_sec_tier) AS prev2_n_is_explicit_true,
        NTILE(10) OVER (PARTITION BY n_is_explicit_true, n_is_explicit_true_distance ORDER BY n_is_explicit_true_plus_total_duration_sec DESC, is_explicit, duration_sec_tier) AS n_is_explicit_true_plus_total_duration_sec_decil,
        ROUND(AVG(release_date_span_days) OVER (PARTITION BY n_is_explicit_true ORDER BY distinct_artist_ids DESC, is_explicit, duration_sec_tier ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS release_date_span_days_avg_3
    FROM tagged
    WINDOW w_distinct_artist_ids AS (PARTITION BY distinct_artist_ids)
),
tagged_2 AS (
    -- Derive helper columns
    SELECT
        is_explicit,
        duration_sec_tier,
        n_is_explicit_true,
        distinct_artist_ids,
        n_is_explicit_true_distance,
        n_is_explicit_true_plus_total_duration_sec,
        least_total_duration_sec_n_is_explicit_true,
        distinct_artist_ids_avg_n_is_explicit_true_dista,
        distinct_artist_ids_share_of_distinct_artist_ids,
        prev2_n_is_explicit_true,
        release_date_span_days_avg_3,
        LOWER(REGEXP_REPLACE(TRIM(duration_sec_tier), ' ', '_')) AS duration_sec_tier_slug,
        UPPER(TRIM(duration_sec_tier)) AS duration_sec_tier_clean
    FROM with_windows
)
-- Final projection
SELECT
    is_explicit,
    distinct_artist_ids,
    n_is_explicit_true_distance,
    n_is_explicit_true_plus_total_duration_sec,
    least_total_duration_sec_n_is_explicit_true,
    distinct_artist_ids_share_of_distinct_artist_ids,
    release_date_span_days_avg_3,
    duration_sec_tier_slug,
    duration_sec_tier_clean,
    LOWER(REGEXP_REPLACE(TRIM(duration_sec_tier), ' ', '_')) AS duration_sec_tier_slug_2
FROM tagged_2
ORDER BY distinct_artist_ids DESC
