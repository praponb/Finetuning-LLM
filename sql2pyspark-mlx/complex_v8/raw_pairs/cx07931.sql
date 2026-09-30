WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM claims
    WHERE (status NOT IN ('REJECTED', 'OPEN') OR approved_amount BETWEEN 0.0 AND 39988.0)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount_minus_claimed_amount, id) AS claimed_amount_change,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY policy_id ORDER BY approved_amount_minus_claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        SUM(claimed_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount_minus_claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        COUNT(*) OVER (PARTITION BY status, claim_type) AS rows_in_status_claim_type,
        NTILE(4) OVER (PARTITION BY policy_id ORDER BY approved_amount_minus_claimed_amount, id) AS approved_amount_minus_claimed_amount_quartile
    FROM cleaned
),
enriched_policy AS (
    -- Enrich with dimension data
    SELECT s.*, p.holder_id AS policy_holder_id, p.start_date AS policy_start_date
    FROM with_windows s
    JOIN policies p ON p.id = s.policy_id
)
-- Subtotals with GROUPING SETS
SELECT
    approved_amount_minus_claimed_amount_quartile,
    adjuster_id,
    policy_holder_id,
    COUNT(DISTINCT policy_id) AS distinct_policy_ids,
    ROUND(SUM(rows_in_status_claim_type) / NULLIF(SUM(approved_amount_minus_claimed_amount_quartile), 0), 3) AS rows_in_status_claim_type_to_approved_amount_min
FROM enriched_policy
GROUP BY GROUPING SETS ((approved_amount_minus_claimed_amount_quartile, adjuster_id, policy_holder_id), (approved_amount_minus_claimed_amount_quartile), ())
ORDER BY approved_amount_minus_claimed_amount_quartile NULLS LAST, adjuster_id NULLS LAST, policy_holder_id NULLS LAST
