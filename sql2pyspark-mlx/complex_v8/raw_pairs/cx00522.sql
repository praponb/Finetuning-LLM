WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        ROUND(claimed_amount * 1.1, 2) AS claimed_amount_adj,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE adjuster_id >= 5
),
enriched_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.claimed_amount_adj,
        s.reported_at_date,
        a.adjuster_name,
        p.end_date AS policy_end_date,
        p.product AS policy_product
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM enriched_adjuster s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_date >= DATE '2024-06-27'
              AND c.approved_amount < 19337.6
        )
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) FILTER (WHERE claim_type IN ('theft', 'flood')) AS claim_claim_type_count,
        COUNT(*) FILTER (WHERE claim_type IS NOT NULL) AS claim_claim_type_count_2,
        ROUND(AVG(approved_amount), 2) AS avg_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count,
        COALESCE(c.claim_claim_type_count_2, 0) AS claim_claim_type_count_2,
        c.avg_claim_approved_amount
    FROM screened s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(claim_claim_type_count_2) OVER (PARTITION BY MONTH(claim_date)) AS claim_date_month_sum_claim_claim_type_count_2,
        LAST_VALUE(policy_end_date) OVER (PARTITION BY claim_claim_type_count ORDER BY claimed_amount_adj ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_policy_end_date,
        COUNT(*) OVER w_claim_date_month AS rows_in_claim_date_month,
        FIRST_VALUE(avg_claim_approved_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount_adj DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_avg_claim_approved_amount
    FROM summarized_rows
    WINDOW w_claim_date_month AS (PARTITION BY MONTH(claim_date))
)
SELECT *
FROM (
    SELECT last_policy_end_date, policy_product, avg_claim_approved_amount
    FROM with_trends
) src
PIVOT (
    COUNT(avg_claim_approved_amount)
    FOR policy_product IN ('TRAVEL', 'HOME')
)
ORDER BY last_policy_end_date
