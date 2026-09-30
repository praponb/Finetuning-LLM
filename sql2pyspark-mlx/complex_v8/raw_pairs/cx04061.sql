WITH prepared_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        ROUND(risk_score * 1.07, 2) AS risk_score_adj,
        LOWER(REGEXP_REPLACE(TRIM(segment), ' ', '_')) AS segment_slug,
        CONCAT(region, '#', CAST(id AS STRING)) AS region_ref
    FROM policyholders
    WHERE region IS NOT NULL
      AND joined_on < DATE '2025-02-22'
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.premium) FROM policies p WHERE p.holder_id = s.id) AS max_policy_premium
    FROM prepared_holders s
),
tagged AS (
    -- Label and score rows
    SELECT *, COALESCE(region, 'UNKNOWN') AS region_or_unknown
    FROM with_stats
    WHERE region = 'East'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        risk_score,
        joined_on,
        risk_score_adj,
        region_ref,
        max_policy_premium,
        region_or_unknown,
        LAG(max_policy_premium, 3) OVER (PARTITION BY MONTH(joined_on) ORDER BY risk_score, id) AS prev3_max_policy_premium,
        MAX(risk_score_adj) OVER (PARTITION BY region, YEAR(joined_on)) AS region_joined_on_year_max_risk_score_adj,
        SUM(max_policy_premium) OVER (PARTITION BY MONTH(joined_on) ORDER BY risk_score, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS max_policy_premium_sum_1_prior
    FROM tagged
),
line_level_policys AS (
    -- One row per policy
    SELECT
        s.id,
        s.risk_score_adj,
        s.max_policy_premium,
        s.prev3_max_policy_premium,
        s.region_joined_on_year_max_risk_score_adj,
        s.max_policy_premium_sum_1_prior,
        p.id AS policy_id,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM sequenced s
    JOIN policies p ON p.holder_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_id, policy_status, prev3_max_policy_premium
    FROM line_level_policys
) src
PIVOT (
    SUM(prev3_max_policy_premium)
    FOR policy_status IN ('CANCELLED', 'LAPSED', 'ACTIVE')
)
