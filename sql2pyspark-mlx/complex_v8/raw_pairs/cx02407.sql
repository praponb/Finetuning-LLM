WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_reported_at
    FROM covered s
)
SELECT
    policy_id,
    adjuster_id,
    product,
    SUM(claimed_amount) FILTER (WHERE product NOT IN ('LIFE', 'TRAVEL')) AS claimed_amount_product_not_in_life_travel,
    COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids
FROM summarized_rows
GROUP BY GROUPING SETS ((policy_id, adjuster_id, product), (policy_id), (adjuster_id))
