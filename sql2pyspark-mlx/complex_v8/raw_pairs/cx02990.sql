WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status
    FROM policies
    WHERE (status NOT IN ('LAPSED', 'CANCELLED') OR product = 'LIFE')
),
labeled AS (
    -- Add calculated columns
    SELECT *, COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM filtered_policys
    WHERE status NOT IN ('CANCELLED', 'ACTIVE')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        end_date,
        LAG(end_date) OVER (PARTITION BY holder_id, status_or_unknown ORDER BY end_date DESC, id) AS prev_end_date,
        LAG(end_date, 2) OVER (PARTITION BY holder_id ORDER BY end_date, id) AS prev2_end_date,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id, status_or_unknown ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date_2
    FROM labeled
),
joined AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.second_end_date_2,
        p.risk_score AS holder_risk_score
    FROM with_trends s
    JOIN policyholders p ON p.id = s.holder_id
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        c.id AS claim_id,
        c.claim_date,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_type,
        c.reported_at AS claim_reported_at
    FROM joined s
    JOIN claims c ON c.policy_id = s.id AND HOUR(c.reported_at) BETWEEN 9 AND 14
),
exploded_detail_2 AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.claim_id,
        s.claim_date,
        s.claim_claimed_amount,
        s.claim_reported_at,
        c.id AS claim_id_2,
        c.claim_date AS claim_claim_date,
        c.status AS claim_status
    FROM exploded_detail s
    JOIN claims c ON c.policy_id = s.id
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM exploded_detail_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.end_date BETWEEN DATE '2024-11-08' AND DATE '2026-09-23'
              AND p.premium >= 2033.8
        )
)
-- Final projection
SELECT
    id,
    holder_id,
    claim_id,
    claim_claimed_amount,
    claim_reported_at,
    claim_id_2,
    ROUND(claim_claimed_amount * 1.5, 2) AS claim_claimed_amount_adj,
    DAYOFWEEK(claim_reported_at) AS claim_reported_at_dayofweek
FROM without_matches
WHERE claim_claimed_amount <= 34763.4
ORDER BY claim_reported_at DESC
