WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE claim_date < DATE '2025-10-29'
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM base_claims s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.claim_type = 'flood'
        )
),
enriched AS (
    -- Join policies
    SELECT
        s.*,
        p.end_date AS policy_end_date,
        po.region AS holder_region,
        po.joined_on AS holder_joined_on
    FROM qualified s
    JOIN policies p ON p.id = s.policy_id AND p.product IS NOT NULL
    JOIN policyholders po ON po.id = p.holder_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN holder_region IN ('North', 'Central') THEN 'group_a' WHEN holder_region = 'East' THEN 'group_b' ELSE 'other' END AS holder_region_group,
        QUARTER(holder_joined_on) AS holder_joined_on_quarter,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM enriched
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.reported_at,
        s.holder_region_group,
        s.holder_joined_on_quarter,
        s.status_slug,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.claimed_amount AS claim_claimed_amount
    FROM tagged s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.policy_id IS NOT NULL
),
enriched_2 AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.reported_at,
        s.holder_region_group,
        s.holder_joined_on_quarter,
        s.status_slug,
        s.claim_id,
        s.claim_policy_id,
        s.claim_claimed_amount,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM detail_rows s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    claimed_amount,
    reported_at,
    holder_region_group,
    holder_joined_on_quarter,
    status_slug,
    claim_id,
    claim_policy_id,
    claim_claimed_amount,
    adjuster_level,
    adjuster_team,
    adjuster_name,
    DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label,
    TO_DATE(reported_at) AS reported_at_date
FROM enriched_2
WHERE claimed_amount BETWEEN 10673.3 AND 32878.5
ORDER BY adjuster_name DESC, id, claim_id
LIMIT 20
