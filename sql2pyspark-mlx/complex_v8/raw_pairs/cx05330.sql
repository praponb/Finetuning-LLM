WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        CASE WHEN duration_sec >= 357 THEN 'large' WHEN duration_sec >= 209 THEN 'medium' ELSE 'small' END AS duration_sec_tier
    FROM tracks
    WHERE SIZE(moods) >= 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(duration_sec_per_bpm - AVG(duration_sec_per_bpm) OVER w_duration_sec_tier, 2) AS duration_sec_per_bpm_vs_duration_sec_tier_avg,
        MAX(duration_sec_per_bpm) OVER (ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_per_bpm_so_far,
        ROUND(duration_sec_per_bpm / NULLIF(SUM(duration_sec_per_bpm) OVER w_duration_sec_tier_is_explici, 0), 4) AS duration_sec_per_bpm_share_of_duration_sec_tier_,
        LAST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        ROUND(CUME_DIST() OVER (ORDER BY bpm, id), 3) AS bpm_cume_dist,
        ROUND(PERCENT_RANK() OVER w_duration_sec_tier_artist_id, 3) AS duration_sec_per_bpm_percent_rank,
        COUNT(*) OVER (PARTITION BY duration_sec_tier) AS rows_in_duration_sec_tier,
        NTH_VALUE(duration_sec_per_bpm, 2) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_per_bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY duration_sec_tier), 0), 4) AS bpm_share_of_duration_sec_tier,
        SUM(duration_sec_per_bpm) OVER (PARTITION BY duration_sec_tier) AS duration_sec_tier_sum_duration_sec_per_bpm,
        MAX(bpm) OVER (ORDER BY duration_sec_per_bpm, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS bpm_max_3
    FROM base
    WINDOW w_duration_sec_tier AS (PARTITION BY duration_sec_tier), w_duration_sec_tier_is_explici AS (PARTITION BY duration_sec_tier, is_explicit), w_duration_sec_tier_artist_id AS (PARTITION BY duration_sec_tier, artist_id ORDER BY duration_sec_per_bpm)
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        rows_in_duration_sec_tier - LAG(rows_in_duration_sec_tier) OVER (ORDER BY bpm, id) AS rows_in_duration_sec_tier_change,
        ROUND(AVG(rows_in_duration_sec_tier) OVER w_duration_sec_tier_is_explici, 2) AS duration_sec_tier_is_explicit_avg_rows_in_durati,
        LAST_VALUE(last_known_bpm) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY duration_sec_per_bpm_vs_duration_sec_tier_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_bpm
    FROM metrics
    WINDOW w_duration_sec_tier_is_explici AS (PARTITION BY duration_sec_tier, is_explicit)
)
SELECT
    id,
    artist_id,
    is_explicit,
    bpm,
    duration_sec_per_bpm,
    duration_sec_tier,
    max_duration_sec_per_bpm_so_far,
    duration_sec_per_bpm_share_of_duration_sec_tier_,
    last_known_bpm,
    bpm_cume_dist,
    second_duration_sec_per_bpm,
    duration_sec_tier_sum_duration_sec_per_bpm,
    bpm_max_3,
    rows_in_duration_sec_tier_change,
    duration_sec_tier_is_explicit_avg_rows_in_durati,
    last_known_last_known_bpm,
    CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
FROM metrics_2
WHERE duration_sec_tier_is_explicit_avg_rows_in_durati BETWEEN 1.9 AND 19.1
ORDER BY is_explicit_flag NULLS LAST, id
LIMIT 5
