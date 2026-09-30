WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE title IS NOT NULL
),
rollup_base AS (
    -- Summarise per group
    SELECT
        CASE WHEN bpm >= 147 THEN 'large' WHEN bpm >= 100 THEN 'medium' ELSE 'small' END AS bpm_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(bpm, 0.5) AS median_bpm,
        MAX(duration_sec) AS max_duration_sec,
        SUM(bpm) FILTER (WHERE is_explicit = FALSE) AS bpm_is_explicit_false
    FROM filtered_tracks
    GROUP BY 1
    HAVING AVG(moods_count) > 1
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        bpm_tier,
        distinct_ids,
        max_duration_sec,
        bpm_is_explicit_false,
        MIN(bpm_is_explicit_false) OVER w_distinct_ids AS distinct_ids_min_bpm_is_explicit_false,
        ROUND(median_bpm - AVG(median_bpm) OVER (PARTITION BY distinct_ids), 2) AS median_bpm_vs_distinct_ids_avg
    FROM rollup_base
    WINDOW w_distinct_ids AS (PARTITION BY distinct_ids)
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(distinct_ids / NULLIF(SUM(distinct_ids) OVER w_bpm_tier, 0), 4) AS distinct_ids_share_of_bpm_tier,
        LAST_VALUE(max_duration_sec) IGNORE NULLS OVER (PARTITION BY distinct_ids ORDER BY bpm_is_explicit_false DESC, bpm_tier ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_max_duration_sec,
        SUM(bpm_is_explicit_false) OVER w_bpm_tier AS bpm_tier_sum_bpm_is_explicit_false,
        COUNT(*) OVER (PARTITION BY bpm_tier) AS rows_in_bpm_tier,
        bpm_is_explicit_false - LAG(bpm_is_explicit_false) OVER w_distinct_ids_bpm_tier_by_dis AS bpm_is_explicit_false_change
    FROM with_trends
    WINDOW w_bpm_tier AS (PARTITION BY bpm_tier), w_distinct_ids_bpm_tier_by_dis AS (PARTITION BY distinct_ids, bpm_tier ORDER BY distinct_ids, bpm_tier)
)
-- Native SQL UNPIVOT (wide to long)
SELECT bpm_tier, field, amount
FROM (
    SELECT bpm_tier, distinct_ids_share_of_bpm_tier, median_bpm_vs_distinct_ids_avg
    FROM with_trends_2
) src
UNPIVOT (
    amount FOR field IN (distinct_ids_share_of_bpm_tier, median_bpm_vs_distinct_ids_avg)
) unp
ORDER BY bpm_tier, field
