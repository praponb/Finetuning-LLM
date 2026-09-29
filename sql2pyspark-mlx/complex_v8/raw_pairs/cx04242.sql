WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status
    FROM policies
    WHERE end_date BETWEEN DATE '2026-01-20' AND DATE '2027-01-03'
      AND holder_id >= 10
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(premium) OVER (PARTITION BY status, product) AS status_product_max_premium,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product, status ORDER BY premium DESC, id), 3) AS premium_percent_rank,
        SUM(premium) OVER (PARTITION BY product, status ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        LAG(premium) OVER (PARTITION BY product, status ORDER BY premium DESC, id) AS prev_premium
    FROM valid_policys
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.prev_premium,
        c.id AS claim_id,
        c.claim_date,
        c.approved_amount AS claim_approved_amount
    FROM ranked s
    JOIN claims c ON c.policy_id = s.id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        prev_premium,
        claim_id,
        claim_date,
        claim_approved_amount,
        ROUND(prev_premium / NULLIF(SUM(prev_premium) OVER (PARTITION BY claim_id, product), 0), 4) AS prev_premium_share_of_claim_id_product,
        SUM(claim_approved_amount) OVER (PARTITION BY claim_id, product) AS claim_id_product_sum_claim_approved_amount,
        FIRST_VALUE(claim_approved_amount) OVER (PARTITION BY claim_id, holder_id ORDER BY end_date DESC NULLS FIRST, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_approved_amount,
        LAG(prev_premium, 3) OVER w_holder_id_product_by_prev_pr AS prev3_prev_premium
    FROM exploded_detail_claims
    WINDOW w_holder_id_product_by_prev_pr AS (PARTITION BY holder_id, product ORDER BY prev_premium ASC, id, claim_id)
)
SELECT
    holder_id,
    COALESCE(product, 'ALL') AS product,
    COUNT(*) AS row_count,
    ROUND(SUM(claim_approved_amount) / NULLIF(SUM(prev_premium), 0), 3) AS claim_approved_amount_to_prev_premium_ratio,
    ROUND(SUM(prev_premium_share_of_claim_id_product), 2) AS total_prev_premium_share_of_claim_id_product
FROM with_windows
GROUP BY GROUPING SETS ((holder_id, product), (holder_id))
