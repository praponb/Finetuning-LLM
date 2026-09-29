WITH recent_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        segment,
        CASE WHEN segment IN ('RETAIL', 'VIP') THEN 'group_a' WHEN segment = 'SME' THEN 'group_b' ELSE 'other' END AS segment_group
    FROM policyholders
    WHERE risk_score < 0.8
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        holder_name,
        region,
        CONCAT_WS(': ', holder_name, region) AS holder_name_region_label,
        CONCAT_WS(': ', segment_group, holder_name) AS segment_group_holder_name_label,
        COALESCE(segment_group, 'UNKNOWN') AS segment_group_or_unknown
    FROM recent_holders
),
scored AS (
    -- Add calculated columns
    SELECT *, CONCAT(region, '#', CAST(id AS STRING)) AS region_ref
    FROM derived
    WHERE region IS NOT NULL
),
line_level AS (
    -- One row per policy
    SELECT
        s.id,
        s.region,
        s.holder_name_region_label,
        s.segment_group_holder_name_label,
        s.segment_group_or_unknown,
        s.region_ref,
        p.id AS policy_id,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM scored s
    JOIN policies p ON p.holder_id = s.id
)
-- Final projection
SELECT
    id,
    region,
    holder_name_region_label,
    segment_group_or_unknown,
    region_ref,
    policy_id,
    policy_start_date
FROM line_level
ORDER BY policy_id DESC
