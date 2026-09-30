WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        ROUND(claimed_amount * 1.5, 2) AS claimed_amount_adj
    FROM claims
    WHERE YEAR(claim_date) = 2024
      AND claim_type = 'medical'
),
combined_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.claimed_amount_adj,
        a.level AS adjuster_level,
        p.product AS policy_product
    FROM filtered_claims s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.level BETWEEN 2 AND 3
    JOIN policies p ON p.id = s.policy_id
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        claimed_amount_adj,
        adjuster_level,
        policy_product,
        adjuster_level + approved_amount AS adjuster_level_plus_approved_amount,
        LOWER(REGEXP_REPLACE(TRIM(policy_product), ' ', '_')) AS policy_product_slug,
        CASE WHEN adjuster_level >= 3 THEN 'A' WHEN adjuster_level >= 2 THEN 'B' ELSE 'C' END AS adjuster_level_tier
    FROM combined_details_adjuster
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.adjuster_level,
        s.policy_product,
        s.adjuster_level_plus_approved_amount,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at
    FROM enriched s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
with_parent AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.adjuster_level,
        s.policy_product,
        s.adjuster_level_plus_approved_amount,
        s.claim_id,
        s.claim_approved_amount,
        s.claim_reported_at,
        a.team AS adjuster_team,
        c.approved_amount AS claim_approved_amount_2,
        c.reported_at AS claim_reported_at_2
    FROM detail_rows s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN claims c ON c.id = s.claim_id
    WHERE a.team = 'Motor'
),
combined_details AS (
    -- Attach claim attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.policy_product,
        s.adjuster_level_plus_approved_amount,
        s.claim_id,
        s.claim_approved_amount_2,
        c.approved_amount AS claim_approved_amount_3,
        c.claim_date,
        p.status AS policy_status
    FROM with_parent s
    JOIN claims c ON c.id = s.claim_id AND c.claimed_amount BETWEEN 6745.1 AND 42447.2
    LEFT JOIN policies p ON p.id = s.policy_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(policy_product, 'ALL') AS policy_product,
    policy_id,
    GROUPING(policy_product) AS is_total_policy_product,
    ARRAY_SORT(COLLECT_SET(policy_status)) AS policy_status_list,
    COUNT(*) AS row_count,
    DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days
FROM combined_details
GROUP BY GROUPING SETS ((policy_product, policy_id), (policy_product))
ORDER BY policy_product NULLS LAST, policy_id NULLS LAST
