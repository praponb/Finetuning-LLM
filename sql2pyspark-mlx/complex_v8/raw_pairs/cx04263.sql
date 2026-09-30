WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.status,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        CASE WHEN product IN ('AUTO', 'LIFE') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group,
        LENGTH(TRIM(status)) AS status_len
    FROM covered
    WHERE claimed_amount >= 5774.6
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium), 3) AS premium_percent_rank,
        MIN(premium) OVER (PARTITION BY product_or_unknown) AS product_or_unknown_min_premium,
        RANK() OVER w_status_policy_id_by_claim_da AS claim_date_rank
    FROM labeled
    WINDOW w_status_policy_id_by_claim_da AS (PARTITION BY status, policy_id ORDER BY claim_date DESC)
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(claim_date_rank - AVG(claim_date_rank) OVER w_status_claim_date_rank, 2) AS claim_date_rank_vs_status_claim_date_rank_avg,
        ROUND(AVG(premium_percent_rank) OVER (PARTITION BY product_or_unknown ORDER BY premium, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_percent_rank_avg_4,
        MIN(claimed_amount) OVER (PARTITION BY status, claim_date_rank) AS status_claim_date_rank_min_claimed_amount,
        RANK() OVER (PARTITION BY product_or_unknown ORDER BY premium) AS premium_rank
    FROM sequenced
    WINDOW w_status_claim_date_rank AS (PARTITION BY status, claim_date_rank)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    adjuster_id,
    product,
    premium,
    product_or_unknown,
    product_group,
    status_len,
    product_or_unknown_min_premium,
    claim_date_rank,
    premium_percent_rank_avg_4,
    status_claim_date_rank_min_claimed_amount,
    premium_rank,
    premium_percent_rank - premium AS premium_percent_rank_minus_premium,
    DATE_TRUNC('WEEK', claim_date) AS claim_date_week
FROM sequenced_2
ORDER BY product_or_unknown_min_premium DESC, claim_date_rank
