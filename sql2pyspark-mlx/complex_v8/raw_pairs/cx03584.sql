WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown_2
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount,
        ROUND(approved_amount * 0.25, 2) AS approved_amount_adj,
        ROUND(approved_amount * 0.9, 2) AS approved_amount_adj_2
    FROM base_claims
    WHERE claim_type IN ('flood', 'theft')
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.status,
        s.status_or_unknown_2,
        s.approved_amount_per_claimed_amount,
        s.approved_amount_adj,
        s.approved_amount_adj_2,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.reported_at AS claim_reported_at
    FROM enriched s
    JOIN claims c ON c.policy_id = s.policy_id
),
screened AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM detail_rows s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.start_date BETWEEN DATE '2024-07-19' AND DATE '2025-05-28'
              AND p.end_date >= DATE '2026-01-23'
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, claim_id, metric_name, amount
FROM (
    SELECT id, claim_id, approved_amount_adj, approved_amount_per_claimed_amount, approved_amount_adj_2
    FROM screened
) src
UNPIVOT (
    amount FOR metric_name IN (approved_amount_adj, approved_amount_per_claimed_amount, approved_amount_adj_2)
) unp
