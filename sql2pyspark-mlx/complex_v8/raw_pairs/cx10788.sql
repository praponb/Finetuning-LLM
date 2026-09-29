WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        ROUND(approved_amount * 1.5, 2) AS approved_amount_adj,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount,
        CASE WHEN status IN ('PAID', 'OPEN') THEN 'group_a' WHEN status = 'APPROVED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_reported_at,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS first_claim_date
    FROM scoped s
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.status,
        s.reported_at,
        s.approved_amount_adj,
        s.status_group,
        s.last_claim_reported_at,
        s.first_claim_date,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM with_counts s
    JOIN adjusters a ON a.id = s.adjuster_id
)
-- Subtotals with GROUPING SETS
SELECT
    policy_id,
    COALESCE(status_group, 'ALL') AS status_group,
    COALESCE(status, 'ALL') AS status,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_details
GROUP BY GROUPING SETS ((policy_id, status_group, status), (policy_id), (status_group), ())
