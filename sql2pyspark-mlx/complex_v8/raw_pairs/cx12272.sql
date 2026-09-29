WITH in_window AS (
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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        claim_date,
        status,
        product,
        premium,
        UPPER(TRIM(product)) AS product_clean,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM in_window
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        status,
        product,
        product_clean,
        DENSE_RANK() OVER (PARTITION BY policy_id, product_or_unknown ORDER BY claim_date) AS claim_date_dense_rank,
        LEAD(premium) OVER (PARTITION BY policy_id, product_or_unknown ORDER BY claim_date, id) AS next_premium
    FROM scored
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ABS(next_premium - 3532.7) AS next_premium_distance,
        CASE WHEN status IN ('OPEN', 'PAID') THEN 'group_a' WHEN status = 'REJECTED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM metrics
    WHERE claim_date >= DATE '2024-04-14'
)
SELECT
    claim_date_dense_rank,
    COALESCE(product, 'ALL') AS product,
    policy_id,
    COUNT(*) AS cnt,
    ROUND(SUM(claim_date_dense_rank) / NULLIF(SUM(next_premium), 0), 3) AS claim_date_dense_rank_to_next_premium_ratio,
    SUM(CASE WHEN product NOT IN ('HEALTH', 'AUTO') THEN claim_date_dense_rank ELSE 0 END) AS claim_date_dense_rank_product_not_in_health_auto
FROM derived
GROUP BY CUBE(claim_date_dense_rank, product, policy_id)
ORDER BY claim_date_dense_rank NULLS LAST, product NULLS LAST, policy_id NULLS LAST
