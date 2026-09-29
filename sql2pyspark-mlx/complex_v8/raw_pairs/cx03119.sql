WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label
    FROM policies
    WHERE product = 'AUTO'
      AND premium BETWEEN 1042.5 AND 3719.7
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        p.segment AS holder_segment
    FROM prepared s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.holder_name IS NOT NULL
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id
    FROM with_parent_holder s
    JOIN claims c ON c.policy_id = s.id AND c.claimed_amount >= 20436.5
)
-- Subtotals with CUBE
SELECT
    claim_id,
    claim_adjuster_id,
    GROUPING_ID() AS gid,
    SUM(CASE WHEN claim_status = 'APPROVED' THEN 1 ELSE 0 END) AS n_claim_status_approved,
    MIN(claim_claimed_amount) AS min_claim_claimed_amount
FROM exploded_detail_claims
GROUP BY CUBE(claim_id, claim_adjuster_id)
ORDER BY claim_id NULLS LAST, claim_adjuster_id NULLS LAST
