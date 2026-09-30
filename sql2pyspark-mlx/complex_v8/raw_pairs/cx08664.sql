WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        CASE WHEN device IN ('phone', 'desktop') THEN 'group_a' WHEN device = 'car' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE source IN ('album', 'radio')
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        device,
        is_skipped,
        source,
        LOWER(TRIM(source)) AS source_clean,
        CONCAT_WS(' / ', device_group, source) AS device_group_source_label
    FROM base
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix,
        LENGTH(TRIM(source_clean)) AS source_clean_len,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM enriched
),
source_stats AS (
    -- Median source_clean_len per source
    SELECT source, PERCENTILE_APPROX(source_clean_len, 0.5) AS median_source_clean_len
    FROM labeled
    GROUP BY source
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_source_clean_len,
        ROUND(s.source_clean_len - g.median_source_clean_len, 2) AS source_clean_len_minus_median
    FROM labeled s
    JOIN source_stats g ON g.source = s.source
    WHERE s.source_clean_len >= g.median_source_clean_len
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM above_average
    WHERE median_source_clean_len > (SELECT MIN(median_source_clean_len) FROM above_average)
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN is_skipped_flag >= 1 THEN 'A' WHEN is_skipped_flag >= 0 THEN 'B' ELSE 'C' END AS is_skipped_flag_tier,
        CASE WHEN median_source_clean_len >= 9.8 THEN 'HIGH' WHEN median_source_clean_len >= 7.4 THEN 'MEDIUM' ELSE 'LOW' END AS median_source_clean_len_tier,
        FLOOR(source_clean_len / 2) * 2 AS source_clean_len_bucket
    FROM qualified
)
-- Final projection
SELECT
    id,
    is_skipped,
    source,
    source_clean,
    device_group_source_label,
    source_clean_len,
    is_skipped_flag,
    median_source_clean_len,
    source_clean_len_minus_median,
    is_skipped_flag_tier,
    median_source_clean_len_tier,
    source_clean_len_bucket
FROM enriched_2
