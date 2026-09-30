WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE claim_date IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_3_prior,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY MONTH(claim_date))) AS distinct_adjuster_id_in_claim_date_month,
        SUM(claimed_amount) OVER (PARTITION BY MONTH(claim_date)) AS claim_date_month_sum_claimed_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY MONTH(claim_date)), 0), 4) AS claimed_amount_share_of_claim_date_month
    FROM valid
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_date >= DATE '2024-04-09'
              AND c.approved_amount <= 15857.9
        )
)
-- Subtotals with GROUPING SETS
SELECT
    claim_type,
    distinct_adjuster_id_in_claim_date_month,
    MIN(reported_at) AS first_reported_at
FROM filtered
GROUP BY GROUPING SETS ((claim_type, distinct_adjuster_id_in_claim_date_month), (claim_type), (distinct_adjuster_id_in_claim_date_month), ())
ORDER BY claim_type NULLS LAST, distinct_adjuster_id_in_claim_date_month NULLS LAST
