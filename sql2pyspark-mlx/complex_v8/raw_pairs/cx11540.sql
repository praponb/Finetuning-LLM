WITH covered AS (
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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claimed_amount,
        claim_type,
        adjuster_id,
        product,
        premium,
        ROW_NUMBER() OVER (PARTITION BY policy_id, claim_type ORDER BY claimed_amount, id) AS seq_no,
        MAX(claimed_amount) OVER (PARTITION BY policy_id, claim_type ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_product, 2) AS claimed_amount_vs_product_avg,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY policy_id, claim_type ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_all_by_premium) AS days_since_prev_claim_date,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id, claim_type ORDER BY claimed_amount, id), 3) AS claimed_amount_cume_dist
    FROM covered
    WINDOW w_product AS (PARTITION BY product), w_all_by_premium AS (ORDER BY premium ASC, id)
),
with_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.product,
        s.seq_no,
        s.max_claimed_amount_so_far,
        s.claimed_amount_vs_product_avg,
        s.second_claimed_amount,
        s.days_since_prev_claim_date,
        s.claimed_amount_cume_dist,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM ranked s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.level BETWEEN 1 AND 3
)
SELECT
    COALESCE(product, 'ALL') AS product,
    adjuster_id,
    GROUPING(product) AS is_total_product,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(max_claimed_amount_so_far) AS total_max_claimed_amount_so_far
FROM with_details
GROUP BY ROLLUP(product, adjuster_id)
