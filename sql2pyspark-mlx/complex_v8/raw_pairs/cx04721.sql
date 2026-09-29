WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
joined AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.approved_amount,
        s.adjuster_id,
        s.product,
        s.premium,
        a.level AS adjuster_level
    FROM covered s
    JOIN adjusters a ON a.id = s.adjuster_id
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) FILTER (WHERE claim_type = 'collision') AS claim_claim_type_count,
        ROUND(AVG(claimed_amount), 2) AS avg_claim_claimed_amount
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count,
        c.avg_claim_claimed_amount
    FROM joined s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
-- Subtotals with GROUPING SETS
SELECT
    product,
    adjuster_id,
    adjuster_level,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list,
    ROUND(SUM(premium), 2) AS total_premium,
    COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids
FROM with_counts
GROUP BY GROUPING SETS ((product, adjuster_id, adjuster_level), (product))
ORDER BY product NULLS LAST, adjuster_id NULLS LAST, adjuster_level NULLS LAST
