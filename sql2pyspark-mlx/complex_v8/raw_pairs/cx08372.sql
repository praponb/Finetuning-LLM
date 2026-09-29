WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        DATE_TRUNC('YEAR', claim_date) AS claim_date_year,
        ROUND(claimed_amount * 0.9, 2) AS claimed_amount_adj
    FROM claims
    WHERE TO_DATE(reported_at) < DATE '2025-02-12'
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.claim_date_year,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.claim_date,
        c.approved_amount AS claim_approved_amount
    FROM cleaned s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.claim_date_year,
        s.claim_id,
        s.claim_policy_id,
        s.claim_date,
        s.claim_approved_amount,
        a.team AS adjuster_team,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date
    FROM with_children s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.claim_policy_id
)
SELECT
    claim_id,
    status,
    policy_id,
    COUNT(*) AS n_rows
FROM joined
GROUP BY GROUPING SETS ((claim_id, status, policy_id), (claim_id))
ORDER BY claim_id NULLS LAST, status NULLS LAST, policy_id NULLS LAST
