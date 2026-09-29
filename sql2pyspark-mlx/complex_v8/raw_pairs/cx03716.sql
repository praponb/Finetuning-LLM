WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.status,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        MAX(claimed_amount) OVER w_product AS product_max_claimed_amount,
        DENSE_RANK() OVER (PARTITION BY policy_id ORDER BY approved_amount, id) AS approved_amount_dense_rank,
        LAG(claim_date) OVER (PARTITION BY policy_id ORDER BY approved_amount, id) AS prev_claim_date
    FROM covered
    WINDOW w_product AS (PARTITION BY product)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(approved_amount_dense_rank / NULLIF(SUM(approved_amount_dense_rank) OVER (PARTITION BY approved_amount_dense_rank), 0), 4) AS approved_amount_dense_rank_share_of_approved_amo,
        premium - LAG(premium) OVER (PARTITION BY status ORDER BY approved_amount_dense_rank, id) AS premium_change,
        ROUND(approved_amount_dense_rank - AVG(approved_amount_dense_rank) OVER (PARTITION BY approved_amount_dense_rank), 2) AS approved_amount_dense_rank_vs_approved_amount_de
    FROM metrics
)
-- Subtotals with CUBE
SELECT
    status,
    approved_amount_dense_rank,
    ROUND(SUM(approved_amount), 2) AS total_approved_amount,
    COUNT(*) AS row_count,
    ROUND(STDDEV(product_max_claimed_amount), 2) AS stddev_product_max_claimed_amount
FROM sequenced
GROUP BY CUBE(status, approved_amount_dense_rank)
ORDER BY status NULLS LAST, approved_amount_dense_rank NULLS LAST
