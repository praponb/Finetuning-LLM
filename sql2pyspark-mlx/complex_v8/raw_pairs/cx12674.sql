WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CASE WHEN approved_amount >= 20843.1 THEN 'A' WHEN approved_amount >= 1273.7 THEN 'B' ELSE 'C' END AS approved_amount_tier,
        CASE WHEN status IN ('OPEN', 'IN_REVIEW') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref
    FROM claims
    WHERE claim_type NOT IN ('collision', 'flood')
      AND claimed_amount < 41760.8
),
joined_policy AS (
    -- Join policies
    SELECT s.*, p.product AS policy_product
    FROM prepared_claims s
    JOIN policies p ON p.id = s.policy_id
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.reported_at,
        s.status_group,
        s.status_ref,
        s.policy_product,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_type AS claim_claim_type
    FROM joined_policy s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count_2
    FROM with_children s
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_date,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count_3
    FROM profiled s
)
-- Subtotals with CUBE
SELECT
    claim_count_3,
    adjuster_id,
    claim_count,
    ROUND(AVG(claim_count_2), 2) AS avg_claim_count_2,
    ARRAY_SORT(COLLECT_SET(claim_claim_type)) AS claim_claim_type_list,
    MIN(claim_count) AS min_claim_count
FROM with_stats
GROUP BY CUBE(claim_count_3, adjuster_id, claim_count)
