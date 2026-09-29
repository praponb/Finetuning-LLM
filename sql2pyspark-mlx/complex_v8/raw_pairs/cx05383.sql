WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claimed_amount,
        approved_amount,
        product,
        NTILE(10) OVER w_all_by_claimed_amount AS claimed_amount_decile,
        SUM(claimed_amount) OVER w_policy_id_status AS policy_id_status_sum_claimed_amount,
        SUM(premium) OVER (ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROUND(PERCENT_RANK() OVER w_all_by_claimed_amount, 3) AS claimed_amount_percent_rank,
        ROW_NUMBER() OVER (ORDER BY claimed_amount ASC, id) AS claimed_amount_seq
    FROM covered
    WINDOW w_all_by_claimed_amount AS (ORDER BY claimed_amount ASC, id), w_policy_id_status AS (PARTITION BY policy_id, status)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(running_premium) OVER (PARTITION BY claimed_amount_seq, product ORDER BY claimed_amount_decile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_running_premium_so_far,
        FIRST_VALUE(claimed_amount_percent_rank) IGNORE NULLS OVER (PARTITION BY claimed_amount_seq, product ORDER BY claimed_amount_decile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount_percent_rank,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY claimed_amount_decile, claimed_amount_seq)) AS distinct_product_in_claimed_amount_decile_claime,
        ROUND(claimed_amount_seq / NULLIF(SUM(claimed_amount_seq) OVER (PARTITION BY claimed_amount_decile, claimed_amount_seq), 0), 4) AS claimed_amount_seq_share_of_claimed_amount_decil,
        DENSE_RANK() OVER (PARTITION BY claimed_amount_seq, claimed_amount_decile ORDER BY claimed_amount_percent_rank ASC) AS claimed_amount_percent_rank_dense_rank
    FROM metrics
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.policy_id = s.policy_id) AS avg_claim_approved_amount,
        (SELECT SUM(c.approved_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS total_claim_approved_amount
    FROM with_trends s
)
-- Final projection
SELECT
    id,
    policy_id,
    claimed_amount,
    approved_amount,
    policy_id_status_sum_claimed_amount,
    running_premium,
    max_running_premium_so_far,
    first_known_claimed_amount_percent_rank,
    distinct_product_in_claimed_amount_decile_claime,
    claimed_amount_seq_share_of_claimed_amount_decil,
    claimed_amount_percent_rank_dense_rank,
    claim_count,
    LEAST(running_premium, claim_count) AS least_running_premium_claim_count
FROM summarized_rows
ORDER BY claimed_amount_percent_rank_dense_rank NULLS LAST, first_known_claimed_amount_percent_rank
