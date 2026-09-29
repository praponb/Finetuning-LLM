WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        CASE WHEN status IN ('IN_REVIEW', 'REJECTED') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug,
        CASE WHEN approved_amount >= 18478.3 THEN 'HIGH' WHEN approved_amount >= 1364.5 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier
    FROM claims
),
enriched_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.*,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.team IN ('Health', 'Motor', 'Property')
),
annotated AS (
    -- Add calculated columns
    SELECT *, FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM enriched_adjuster
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.team AS adjuster_team_2,
        p.status AS policy_status,
        p.product AS policy_product
    FROM annotated s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    reported_at,
    status_group,
    claim_type_slug,
    approved_amount_tier,
    adjuster_level,
    adjuster_team,
    adjuster_name,
    claimed_amount_bucket,
    adjuster_team_2,
    policy_status,
    policy_product,
    CONCAT_WS('-', approved_amount_tier, adjuster_team) AS approved_amount_tier_adjuster_team_label
FROM enriched
