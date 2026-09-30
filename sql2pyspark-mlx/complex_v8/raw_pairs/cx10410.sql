WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        status
    FROM policies
    WHERE holder_id >= 14
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type,
        c.claim_date
    FROM filtered_policys s
    JOIN claims c ON c.policy_id = s.id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(claim_claimed_amount / NULLIF(SUM(claim_claimed_amount) OVER (PARTITION BY product, claim_adjuster_id), 0), 4) AS claim_claimed_amount_share_of_product_claim_adju,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_all_by_claim_date) AS days_since_prev_claim_date
    FROM detail_rows
    WINDOW w_all_by_claim_date AS (ORDER BY claim_date DESC, id, claim_id)
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type IS NOT NULL
              AND c.reported_at < TIMESTAMP '2025-03-08 09:00:00'
        )
      AND s.claim_claimed_amount_share_of_product_claim_adju BETWEEN 0.2 AND 0.9
)
SELECT id, claim_id, metric, value
FROM (
    SELECT id, claim_id, claim_claimed_amount_share_of_product_claim_adju, claim_claimed_amount
    FROM without_matches
) src
UNPIVOT (
    value FOR metric IN (claim_claimed_amount_share_of_product_claim_adju AS m_claim_claimed_amount_share_of_product_claim_ad, claim_claimed_amount AS claim_claimed_amount_v)
) unp
