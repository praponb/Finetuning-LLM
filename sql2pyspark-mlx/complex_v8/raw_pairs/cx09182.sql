WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount,
        INITCAP(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE (policy_id <= 20 OR status = 'APPROVED')
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.claimed_amount_minus_approved_amount,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.product AS policy_product
    FROM valid_claims s
    JOIN adjusters a ON a.id = s.adjuster_id AND UPPER(TRIM(a.adjuster_name)) LIKE 'G%'
    LEFT JOIN policies p ON p.id = s.policy_id
    WHERE a.level <= 3
)
-- Subtotals with CUBE
SELECT
    policy_id,
    adjuster_id,
    GROUPING(policy_id) AS is_total_policy_id,
    MAX(adjuster_level) AS max_adjuster_level,
    ROUND(SUM(approved_amount), 2) AS total_approved_amount,
    COUNT(*) AS row_count
FROM enriched
GROUP BY CUBE(policy_id, adjuster_id)
ORDER BY policy_id NULLS LAST, adjuster_id NULLS LAST
