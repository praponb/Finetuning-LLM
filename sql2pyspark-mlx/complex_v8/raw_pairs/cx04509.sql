WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM policies
    WHERE YEAR(start_date) = 2025
      AND status <> 'LAPSED'
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered_policys s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
      AND s.product = 'LIFE'
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_filled,
        s.days_end_date_to_start_date,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status
    FROM screened s
    JOIN claims c ON c.policy_id = s.id
)
-- Subtotals with ROLLUP
SELECT
    claim_adjuster_id,
    holder_id,
    PERCENTILE_APPROX(premium_filled, 0.5) AS median_premium_filled,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE claim_status IN ('PAID', 'REJECTED')) AS n_claim_status_in_paid_rejecte
FROM with_children
GROUP BY ROLLUP(claim_adjuster_id, holder_id)
ORDER BY claim_adjuster_id NULLS LAST, holder_id NULLS LAST
