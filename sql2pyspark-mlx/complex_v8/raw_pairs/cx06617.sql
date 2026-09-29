WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.status,
        s.adjuster_id,
        s.product,
        s.premium,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM covered s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.level <= 3
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.status,
        s.adjuster_id,
        s.product,
        s.adjuster_level,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount
    FROM with_details s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
adjuster_id_stats AS (
    -- Median adjuster_level per adjuster_id
    SELECT adjuster_id, PERCENTILE_APPROX(adjuster_level, 0.5) AS median_adjuster_level
    FROM line_level_claims
    GROUP BY adjuster_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_adjuster_level,
        ROUND(s.adjuster_level - g.median_adjuster_level, 2) AS adjuster_level_minus_median
    FROM line_level_claims s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.adjuster_level >= g.median_adjuster_level
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_reported_at, adjuster_id, status, claim_claimed_amount
    FROM above_average
) src
PIVOT (
    COUNT(claim_claimed_amount) AS count_v, MAX(claim_claimed_amount) AS max_v
    FOR status IN ('IN_REVIEW', 'REJECTED', 'APPROVED', 'PAID')
)
ORDER BY claim_reported_at, adjuster_id
