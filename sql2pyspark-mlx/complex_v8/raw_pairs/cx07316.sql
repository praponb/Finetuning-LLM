WITH filtered_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        DATEDIFF(DATE '2025-06-30', joined_on) AS days_since_joined_on,
        CONCAT(segment, '#', CAST(id AS STRING)) AS segment_ref,
        SUBSTRING(TRIM(segment), 1, 4) AS segment_prefix
    FROM policyholders
    WHERE joined_on IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(risk_score) OVER (PARTITION BY region ORDER BY days_since_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_risk_score,
        LAST_VALUE(risk_score) OVER (PARTITION BY region ORDER BY days_since_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_risk_score,
        ROUND(days_since_joined_on - AVG(days_since_joined_on) OVER (PARTITION BY segment), 2) AS days_since_joined_on_vs_segment_avg
    FROM filtered_holders
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        region,
        risk_score,
        segment,
        segment_ref,
        first_risk_score,
        ABS(first_risk_score - 0.3) AS first_risk_score_distance,
        SUBSTRING(TRIM(segment_ref), 1, 2) AS segment_ref_prefix,
        CONCAT(holder_name, '#', CAST(days_since_joined_on AS STRING)) AS holder_name_ref
    FROM metrics
),
stats AS (
    -- Group-level totals
    SELECT
        segment,
        COUNT(*) AS row_count,
        ROUND(100 * COUNT(CASE WHEN segment IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_segment_is_not_null,
        PERCENTILE_APPROX(first_risk_score_distance, 0.5) AS median_first_risk_score_distance,
        ARRAY_SORT(COLLECT_SET(segment)) AS segment_list
    FROM labeled
    GROUP BY segment
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(pct_segment_is_not_null) OVER (PARTITION BY row_count, segment), 2) AS row_count_segment_avg_pct_segment_is_not_null,
        ROUND(row_count / NULLIF(SUM(row_count) OVER w_row_count_segment, 0), 4) AS row_count_share_of_row_count_segment,
        COUNT(*) OVER (PARTITION BY row_count, segment) AS rows_in_row_count_segment,
        NTH_VALUE(pct_segment_is_not_null, 2) OVER (PARTITION BY segment, row_count ORDER BY median_first_risk_score_distance DESC, segment ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_pct_segment_is_not_null
    FROM stats
    WINDOW w_row_count_segment AS (PARTITION BY row_count, segment)
)
-- Subtotals with GROUPING SETS
SELECT
    row_count,
    segment,
    ROUND(AVG(row_count), 2) AS avg_row_count,
    COUNT(*) AS row_count_2
FROM with_windows
GROUP BY GROUPING SETS ((row_count, segment), (row_count), (segment))
