WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref
    FROM plays
    WHERE device <> 'tv'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        LAST_VALUE(ms_played) OVER (PARTITION BY track_id, listener_id ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        MAX(ms_played) OVER (PARTITION BY source ORDER BY ms_played ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ms_played_max_2_prior
    FROM base
),
per_group AS (
    -- Group-level totals
    SELECT
        is_skipped,
        track_id,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE is_skipped = FALSE) AS n_is_skipped_false,
        PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played,
        ROUND(SUM(ms_played_max_2_prior) / NULLIF(SUM(last_ms_played), 0), 3) AS ms_played_max_2_prior_to_last_ms_played_ratio
    FROM with_windows
    GROUP BY is_skipped, track_id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN median_ms_played >= 281500 THEN 'large' WHEN median_ms_played >= 182750 THEN 'medium' ELSE 'small' END AS median_ms_played_tier
    FROM per_group
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at
    FROM enriched s
),
derived AS (
    -- Derive helper columns
    SELECT
        is_skipped,
        track_id,
        distinct_ids,
        n_is_skipped_false,
        ms_played_max_2_prior_to_last_ms_played_ratio,
        median_ms_played_tier,
        QUARTER(last_played_at) AS last_played_at_quarter
    FROM with_stats
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM derived
    WHERE n_is_skipped_false > (SELECT MIN(n_is_skipped_false) FROM derived)
)
-- Native SQL UNPIVOT (wide to long)
SELECT is_skipped, track_id, metric, value
FROM (
    SELECT is_skipped, track_id, n_is_skipped_false, last_played_at_quarter, distinct_ids
    FROM qualified
) src
UNPIVOT (
    value FOR metric IN (n_is_skipped_false, last_played_at_quarter, distinct_ids)
) unp
ORDER BY is_skipped, track_id, metric
