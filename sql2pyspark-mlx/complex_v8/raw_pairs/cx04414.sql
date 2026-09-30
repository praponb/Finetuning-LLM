WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        DATE_TRUNC('WEEK', joined_on) AS joined_on_week
    FROM policyholders
    WHERE risk_score > 0.2
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        risk_score,
        joined_on,
        segment,
        joined_on_week,
        SIZE(COLLECT_SET(region) OVER (PARTITION BY segment)) AS distinct_region_in_segment,
        NTH_VALUE(joined_on, 2) OVER (PARTITION BY region, segment ORDER BY risk_score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_joined_on
    FROM prepared
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('WEEK', second_joined_on) AS second_joined_on_week,
        LENGTH(TRIM(segment)) AS segment_len,
        CASE WHEN risk_score >= 0.6 THEN 'HIGH' WHEN risk_score >= 0.3 THEN 'MEDIUM' ELSE 'LOW' END AS risk_score_tier
    FROM windowed
    WHERE risk_score >= 0.2
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(second_joined_on_week, 'yyyy-MM') AS second_joined_on_week_ym,
        COUNT(*) AS cnt,
        MAX(joined_on) AS last_joined_on,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT segment) AS distinct_segments
    FROM enriched
    GROUP BY DATE_FORMAT(second_joined_on_week, 'yyyy-MM')
)
SELECT
    second_joined_on_week_ym,
    cnt,
    last_joined_on,
    distinct_ids,
    distinct_segments
FROM per_group
WHERE distinct_ids < 8
