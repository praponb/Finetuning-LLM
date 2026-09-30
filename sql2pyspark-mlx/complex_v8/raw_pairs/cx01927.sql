WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date,
        ABS(premium - 2319.8) AS premium_distance
    FROM policies
    WHERE end_date >= DATE '2025-11-20'
      AND status NOT IN ('EXPIRED', 'LAPSED')
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        c.id AS claim_id,
        c.claim_type,
        c.claimed_amount AS claim_claimed_amount
    FROM prepared s
    LEFT JOIN claims c ON c.policy_id = s.id
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_children_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.end_date IS NOT NULL
        )
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.claim_id,
        s.claim_claimed_amount,
        c.id AS claim_id_2,
        c.claim_date,
        c.claimed_amount AS claim_claimed_amount_2
    FROM without_matches s
    JOIN claims c ON c.policy_id = s.id
)
SELECT
    id,
    holder_id,
    claim_claimed_amount,
    claim_id_2,
    claim_date,
    claim_claimed_amount_2,
    CASE WHEN claim_claimed_amount >= 32878.5 THEN 'HIGH' WHEN claim_claimed_amount >= 19337.6 THEN 'MEDIUM' ELSE 'LOW' END AS claim_claimed_amount_tier
FROM detail_rows
