WITH scoped_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        SUBSTRING(TRIM(region), 1, 2) AS region_prefix,
        CASE WHEN region IN ('Central', 'East') THEN 'group_a' WHEN region = 'South' THEN 'group_b' ELSE 'other' END AS region_group
    FROM policyholders
    WHERE risk_score > 0.1
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        region,
        risk_score,
        joined_on,
        region_prefix,
        region_group,
        DATE_FORMAT(joined_on, 'yyyy-MM') AS joined_on_label,
        ABS(risk_score - 0.3) AS risk_score_distance
    FROM scoped_holders
),
detail_rows AS (
    -- One row per policy
    SELECT
        s.id,
        s.risk_score,
        s.joined_on,
        s.region_prefix,
        s.region_group,
        s.joined_on_label,
        p.id AS policy_id,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date
    FROM tagged s
    JOIN policies p ON p.holder_id = s.id
)
-- Final projection
SELECT
    id,
    risk_score,
    region_prefix,
    region_group,
    joined_on_label,
    policy_premium,
    policy_end_date
FROM detail_rows
ORDER BY joined_on_label
