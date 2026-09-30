WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM matched
    WHERE claimed_amount < (SELECT MAX(claimed_amount) FROM matched)
),
enriched AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.adjuster_id,
        s.product,
        s.premium,
        p.premium AS policy_premium,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date
    FROM without_matches s
    JOIN policies p ON p.id = s.policy_id
),
totals AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        COUNT(*) AS row_count,
        MAX(policy_end_date) AS last_policy_end_date,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        COUNT(*) FILTER (WHERE product <> 'HOME') AS n_product_home,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount
    FROM enriched
    GROUP BY adjuster_id
    HAVING AVG(policy_premium) > 1132.1
)
SELECT
    adjuster_id,
    row_count,
    last_policy_end_date,
    distinct_adjuster_ids,
    n_product_home,
    avg_claimed_amount
FROM totals
ORDER BY distinct_adjuster_ids DESC NULLS FIRST, adjuster_id
LIMIT 10
