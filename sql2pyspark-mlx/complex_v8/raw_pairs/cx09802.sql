WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(ms_played) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        ms_played - LAG(ms_played) OVER (PARTITION BY listener_id, source ORDER BY played_at, id) AS ms_played_change,
        FIRST_VALUE(ms_played_filled) IGNORE NULLS OVER (ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played_filled,
        LEAD(ms_played_filled, 2) OVER (ORDER BY ms_played DESC, id) AS next2_ms_played_filled
    FROM valid_plays
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        is_skipped,
        source,
        SUM(CASE WHEN source = 'search' THEN ms_played_filled ELSE 0 END) AS ms_played_filled_source_search,
        MIN(ms_played) AS min_ms_played,
        COUNT(*) FILTER (WHERE source <> 'search') AS n_source_search
    FROM with_windows
    GROUP BY is_skipped, source
    HAVING MAX(first_known_ms_played_filled) > 163000
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        is_skipped,
        source,
        n_source_search,
        SUM(n_source_search) OVER (PARTITION BY is_skipped, n_source_search) AS is_skipped_n_source_search_sum_n_source_search,
        ROUND(AVG(n_source_search) OVER (ORDER BY ms_played_filled_source_search DESC NULLS FIRST, is_skipped, source ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS n_source_search_avg_2_prior
    FROM per_group
),
annotated AS (
    -- Derive helper columns
    SELECT
        is_skipped,
        source,
        n_source_search_avg_2_prior,
        CONCAT(source, '#', CAST(n_source_search AS STRING)) AS source_ref,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM with_trends
),
latest_per_key AS (
    -- Keep the earliest row per source_or_unknown
    SELECT
        is_skipped,
        source,
        n_source_search_avg_2_prior,
        source_ref,
        is_skipped_label,
        source_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source_or_unknown ORDER BY n_source_search_avg_2_prior, is_skipped, source) AS rn
        FROM annotated
    ) ranked
    WHERE rn = 1
),
labeled AS (
    -- Derive helper columns
    SELECT
        is_skipped,
        source,
        n_source_search_avg_2_prior,
        source_ref,
        is_skipped_label,
        SUBSTRING(TRIM(is_skipped_label), 1, 4) AS is_skipped_label_prefix,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        COALESCE(source_or_unknown, 'UNKNOWN') AS source_or_unknown_or_unknown
    FROM latest_per_key
    WHERE source_ref IS NOT NULL
)
-- Subtotals with GROUPING SETS
SELECT
    is_skipped,
    is_skipped_flag,
    source_or_unknown_or_unknown,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT source_or_unknown_or_unknown) AS distinct_source_or_unknown_or_unknowns
FROM labeled
GROUP BY GROUPING SETS ((is_skipped, is_skipped_flag, source_or_unknown_or_unknown), (is_skipped), ())
ORDER BY is_skipped NULLS LAST, is_skipped_flag NULLS LAST, source_or_unknown_or_unknown NULLS LAST
