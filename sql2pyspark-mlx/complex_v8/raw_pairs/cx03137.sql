WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ABS(premium - 3521.8) AS premium_distance
    FROM policies
    WHERE product NOT IN ('HEALTH', 'TRAVEL')
      AND status = 'CANCELLED'
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        p.joined_on AS holder_joined_on
    FROM filtered s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM combined_details_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.product = 'LIFE'
        )
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.holder_joined_on,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.adjuster_id AS claim_adjuster_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_date
    FROM screened s
    JOIN claims c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    status,
    holder_joined_on,
    claim_id,
    claim_adjuster_id,
    claim_approved_amount
FROM with_children_claims
WHERE YEAR(holder_joined_on) = 2025
ORDER BY status DESC
