WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        ABS(premium - 3465.4) AS premium_distance
    FROM policies
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium_filled,
        s.premium_distance,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_date,
        c.reported_at AS claim_reported_at
    FROM cleaned s
    JOIN claims c ON c.policy_id = s.id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium_filled,
        s.premium_distance,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_date,
        s.claim_reported_at,
        p.holder_name,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM detail_rows_claims s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(premium_distance - AVG(premium_distance) OVER (PARTITION BY claim_adjuster_id, YEAR(claim_date)), 2) AS premium_distance_vs_claim_adjuster_id_claim_date,
        FIRST_VALUE(claim_date) OVER (ORDER BY claim_date, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        NTILE(10) OVER (ORDER BY holder_risk_score, id, claim_id) AS holder_risk_score_decile
    FROM combined_details
)
-- Final projection
SELECT
    id,
    holder_id,
    premium_filled,
    premium_distance,
    claim_id,
    claim_date,
    claim_reported_at,
    holder_name,
    holder_joined_on,
    holder_risk_score,
    premium_distance_vs_claim_adjuster_id_claim_date,
    holder_risk_score_decile
FROM windowed
WHERE claim_adjuster_id IS NOT NULL
