WITH joined_ranges AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
deduped AS (
    -- Keep the latest row per policy_id, product
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, product ORDER BY approved_amount DESC, id DESC) AS rn
        FROM joined_ranges
    ) ranked
    WHERE rn = 1
),
policy_id_stats AS (
    -- Avg premium per policy_id
    SELECT policy_id, AVG(premium) AS avg_premium
    FROM deduped
    GROUP BY policy_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM deduped s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.premium > g.avg_premium
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    approved_amount,
    status,
    premium,
    avg_premium,
    premium_minus_avg,
    CONCAT(product, '#', CAST(policy_id AS STRING)) AS product_ref
FROM vs_group
ORDER BY avg_premium
