WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        CASE WHEN claimed_amount >= 32796.4 THEN 'large' WHEN claimed_amount >= 13635.1 THEN 'medium' ELSE 'small' END AS claimed_amount_tier,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
    FROM matched
    WHERE premium <= 3532.7
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MIN(approved_amount) AS min_claim_approved_amount,
        MAX(claim_date) AS last_claim_date
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_approved_amount,
        c.last_claim_date
    FROM annotated s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY MONTH(last_claim_date))) AS distinct_adjuster_id_in_last_claim_date_month,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY MONTH(last_claim_date)), 2) AS claimed_amount_vs_last_claim_date_month_avg,
        NTILE(4) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id) AS claimed_amount_quartile
    FROM with_stats
)
SELECT
    claimed_amount_quartile,
    adjuster_id,
    GROUPING(claimed_amount_quartile) AS is_total_claimed_amount_quartile,
    ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
FROM with_trends
GROUP BY GROUPING SETS ((claimed_amount_quartile, adjuster_id), (claimed_amount_quartile), (adjuster_id))
