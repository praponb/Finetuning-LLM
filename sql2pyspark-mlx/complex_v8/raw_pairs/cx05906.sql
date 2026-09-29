WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE approved_amount > 2887.3
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        p.premium AS policy_premium
    FROM scoped s
    LEFT JOIN policies p ON p.id = s.policy_id
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.policy_premium,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claim_type AS claim_claim_type
    FROM combined_details s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(policy_premium) OVER (ORDER BY policy_premium DESC, id, claim_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS policy_premium_avg_3_prior,
        LEAD(policy_premium) OVER w_all_by_policy_premium AS next_policy_premium
    FROM line_level_claims
    WINDOW w_all_by_policy_premium AS (ORDER BY policy_premium DESC, id, claim_id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(policy_premium_avg_3_prior) OVER w_claim_id, 2) AS claim_id_avg_policy_premium_avg_3_prior,
        LAG(reported_at) OVER (PARTITION BY status ORDER BY policy_premium ASC NULLS LAST, id, claim_id) AS prev_reported_at,
        NTILE(4) OVER (PARTITION BY status ORDER BY policy_premium ASC NULLS LAST, id, claim_id) AS policy_premium_quartile
    FROM metrics
    WINDOW w_claim_id AS (PARTITION BY claim_id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT reported_at, policy_premium_quartile, status, policy_premium
    FROM with_trends
) src
PIVOT (
    SUM(policy_premium) AS amount, COUNT(policy_premium) AS n
    FOR status IN ('IN_REVIEW', 'OPEN')
)
ORDER BY reported_at, policy_premium_quartile
