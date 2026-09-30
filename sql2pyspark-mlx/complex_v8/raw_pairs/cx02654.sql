WITH cleaned_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        CONCAT_WS(': ', region, holder_name) AS region_holder_name_label,
        DATEDIFF(DATE '2025-06-30', joined_on) AS days_since_joined_on
    FROM policyholders
    WHERE joined_on IS NOT NULL
),
per_group AS (
    -- Summarise per group
    SELECT
        segment,
        DATE_TRUNC('MONTH', joined_on) AS joined_on_month,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(risk_score) / NULLIF(SUM(days_since_joined_on), 0), 3) AS risk_score_to_days_since_joined_on_ratio,
        ROUND(AVG(days_since_joined_on), 2) AS avg_days_since_joined_on,
        SUM(CASE WHEN segment IS NOT NULL THEN 1 ELSE 0 END) AS n_segment_is_not_null,
        COUNT(DISTINCT segment) AS distinct_segments
    FROM cleaned_holders
    GROUP BY 1, 2
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(distinct_ids) OVER (PARTITION BY distinct_segments) AS distinct_segments_sum_distinct_ids,
        ROW_NUMBER() OVER (PARTITION BY distinct_segments, distinct_ids ORDER BY n_segment_is_not_null DESC, segment, joined_on_month) AS seq_no,
        ROUND(AVG(n_segment_is_not_null) OVER (PARTITION BY distinct_segments), 2) AS distinct_segments_avg_n_segment_is_not_null,
        SIZE(COLLECT_SET(segment) OVER (PARTITION BY distinct_segments)) AS distinct_segment_in_distinct_segments,
        ROUND(AVG(distinct_ids) OVER (ORDER BY avg_days_since_joined_on DESC NULLS FIRST, segment, joined_on_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS distinct_ids_avg_3_prior
    FROM per_group
)
-- Native SQL UNPIVOT (wide to long)
SELECT segment, joined_on_month, measure, value
FROM (
    SELECT segment, joined_on_month, risk_score_to_days_since_joined_on_ratio, distinct_segments_avg_n_segment_is_not_null, avg_days_since_joined_on
    FROM ranked
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (risk_score_to_days_since_joined_on_ratio, distinct_segments_avg_n_segment_is_not_null, avg_days_since_joined_on)
) unp
