WITH in_window AS (
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
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        claim_date,
        claimed_amount,
        adjuster_id,
        product,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY product), 0), 4) AS claimed_amount_share_of_product,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id ORDER BY approved_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_so_far,
        SUM(approved_amount) OVER (PARTITION BY product) AS product_sum_approved_amount
    FROM in_window
),
with_details_adjuster AS (
    -- Join adjusters
    SELECT
        s.*,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM windowed s
    JOIN adjusters a ON a.id = s.adjuster_id
)
-- Final projection
SELECT
    id,
    claim_date,
    claimed_amount,
    claimed_amount_share_of_product,
    max_approved_amount_so_far,
    product_sum_approved_amount,
    adjuster_level,
    MONTH(claim_date) AS claim_date_month
FROM with_details_adjuster
WHERE adjuster_id IS NOT NULL
ORDER BY id, product_sum_approved_amount DESC
