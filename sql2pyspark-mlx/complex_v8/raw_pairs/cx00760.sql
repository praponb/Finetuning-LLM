WITH recent_holders AS (
    -- Filter policyholders
    SELECT
        id,
        risk_score,
        joined_on,
        segment,
        CASE WHEN risk_score >= 0.6 THEN 'large' WHEN risk_score >= 0.3 THEN 'medium' ELSE 'small' END AS risk_score_tier,
        CONCAT_WS(': ', region, segment) AS region_segment_label
    FROM policyholders
    WHERE segment IN ('RETAIL', 'SME')
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        joined_on,
        risk_score_tier,
        DATE_FORMAT(joined_on, 'MMM yyyy') AS joined_on_label,
        SUBSTRING(TRIM(region_segment_label), 1, 3) AS region_segment_label_prefix
    FROM recent_holders
    WHERE risk_score < 0.8
),
totals AS (
    -- Group-level totals
    SELECT
        YEAR(joined_on) AS joined_on_year,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(joined_on) AS first_joined_on,
        MAX(joined_on) AS last_joined_on
    FROM annotated
    GROUP BY 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT joined_on_year, field, reading
FROM (
    SELECT joined_on_year, distinct_ids, row_count
    FROM totals
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (distinct_ids AS distinct_ids_v, row_count AS row_count_v)
) unp
