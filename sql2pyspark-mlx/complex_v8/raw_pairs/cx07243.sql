WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        CASE WHEN premium >= 3521.8 THEN 'large' WHEN premium >= 1541.8 THEN 'medium' ELSE 'small' END AS premium_tier,
        CASE WHEN premium >= 3521.8 THEN 'large' WHEN premium >= 2033.8 THEN 'medium' ELSE 'small' END AS premium_tier_2,
        INITCAP(TRIM(status)) AS status_clean
    FROM policies
    WHERE start_date IS NOT NULL
      AND product = 'HEALTH'
),
with_activity AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status NOT IN ('ACTIVE', 'LAPSED')
        )
      AND s.premium > 1051.9
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        product,
        end_date,
        status,
        premium_tier,
        status_clean,
        ABS(premium - 2033.8) AS premium_distance
    FROM with_activity
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        premium_tier,
        status_clean,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM derived
    WHERE end_date < DATE '2026-12-05'
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.status_clean,
        s.product_slug,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claimed_amount AS claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.reported_at AS claim_reported_at
    FROM labeled s
    LEFT JOIN claims c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    status_clean,
    claim_id,
    claim_status,
    claim_claimed_amount,
    claim_adjuster_id,
    claim_reported_at
FROM with_children_claims
ORDER BY claim_adjuster_id DESC
