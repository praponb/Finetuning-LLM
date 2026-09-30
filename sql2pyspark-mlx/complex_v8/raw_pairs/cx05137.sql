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
adjuster_id_stats AS (
    -- Avg claimed_amount per adjuster_id
    SELECT adjuster_id, AVG(claimed_amount) AS avg_claimed_amount
    FROM covered
    GROUP BY adjuster_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount,
        ROUND(s.claimed_amount - g.avg_claimed_amount, 2) AS claimed_amount_minus_avg
    FROM covered s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.claimed_amount > g.avg_claimed_amount
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN c.status NOT IN ('REJECTED', 'OPEN') THEN 1 END) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_status_count
    FROM outliers s
),
unique_rows AS (
    -- Keep the earliest row per product
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        status,
        adjuster_id,
        product,
        premium,
        avg_claimed_amount,
        claimed_amount_minus_avg,
        claim_status_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY claim_status_count, id) AS rn
        FROM summarized_rows
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    status,
    adjuster_id,
    product,
    premium,
    claim_status_count
FROM unique_rows
ORDER BY claimed_amount DESC NULLS FIRST
