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
        CASE WHEN claimed_amount >= 40638.4 THEN 'large' WHEN claimed_amount >= 19347.2 THEN 'medium' ELSE 'small' END AS claimed_amount_tier
    FROM claims
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        HOUR(reported_at) AS reported_at_hour,
        CASE WHEN approved_amount >= 19337.6 THEN 'HIGH' WHEN approved_amount >= 799.5 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier
    FROM cleaned
    WHERE policy_id >= 5
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM derived s
    JOIN adjusters a ON a.id = s.adjuster_id
),
qualified AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM with_details s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.team = 'Motor'
              AND a.level <= 3
        )
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.approved_amount_tier,
        s.adjuster_name,
        s.adjuster_team,
        s.adjuster_level,
        p.product AS policy_product
    FROM qualified s
    JOIN policies p ON p.id = s.policy_id AND p.premium > 2289.0
    WHERE p.product IS NOT NULL
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        s.adjuster_team,
        s.adjuster_level,
        s.policy_product,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.approved_amount AS claim_approved_amount
    FROM with_parent s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.policy_id >= 16
)
SELECT
    id,
    policy_id,
    claim_type,
    status,
    policy_product,
    claim_id,
    claim_policy_id,
    ABS(claim_approved_amount - 17759.3) AS claim_approved_amount_distance,
    COALESCE(policy_product, 'UNKNOWN') AS policy_product_or_unknown
FROM detail_rows_claims
ORDER BY claim_approved_amount_distance NULLS LAST, id, claim_id
LIMIT 20
