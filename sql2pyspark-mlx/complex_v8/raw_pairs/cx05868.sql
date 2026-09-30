WITH scoped_holders AS (
    -- Filter policyholders
    SELECT *
    FROM policyholders
    WHERE joined_on >= DATE '2024-10-23'
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM scoped_holders
    WHERE risk_score < (SELECT MAX(risk_score) FROM scoped_holders)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        region,
        risk_score,
        joined_on,
        LEAD(joined_on) OVER w_segment_region_by_risk_score AS next_joined_on,
        NTH_VALUE(joined_on, 2) OVER (PARTITION BY segment, region ORDER BY risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_joined_on,
        ROUND(AVG(risk_score) OVER (PARTITION BY segment, region ORDER BY risk_score, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS risk_score_avg_1_prior,
        ROUND(CUME_DIST() OVER (ORDER BY risk_score DESC), 3) AS risk_score_cume_dist,
        LAG(joined_on, 3) OVER (ORDER BY risk_score DESC, id) AS prev3_joined_on,
        MAX(risk_score) OVER (PARTITION BY segment, region ORDER BY risk_score, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS risk_score_max_2_prior
    FROM with_activity
    WINDOW w_segment_region_by_risk_score AS (PARTITION BY segment, region ORDER BY risk_score, id)
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count
    FROM windowed s
    LEFT JOIN policy_stats p ON p.holder_id = s.id
),
filtered AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND YEAR(p.start_date) = 2025
              AND p.product = 'TRAVEL'
        )
)
-- Final projection
SELECT
    id,
    region,
    risk_score,
    next_joined_on,
    second_joined_on,
    risk_score_avg_1_prior,
    risk_score_cume_dist,
    risk_score_max_2_prior,
    CASE WHEN region IN ('West', 'North') THEN 'group_a' WHEN region = 'South' THEN 'group_b' ELSE 'other' END AS region_group,
    DATE_TRUNC('QUARTER', next_joined_on) AS next_joined_on_quarter
FROM filtered
