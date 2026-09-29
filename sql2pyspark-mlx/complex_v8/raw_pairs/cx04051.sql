WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.status,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM joined_ranges s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
      AND s.claimed_amount > 18478.3
),
summary AS (
    -- Group-level totals
    SELECT
        policy_id,
        status,
        claim_type,
        COUNT(*) AS row_count,
        MAX(claimed_amount) AS max_claimed_amount,
        SUM(CASE WHEN product = 'AUTO' THEN 1 ELSE 0 END) AS n_product_auto,
        ROUND(STDDEV(claimed_amount), 2) AS stddev_claimed_amount
    FROM filtered
    WHERE premium > 2289.0
    GROUP BY policy_id, status, claim_type
    HAVING MAX(claimed_amount) > 18364.1
),
enriched AS (
    -- Attach policy attributes
    SELECT
        s.policy_id,
        s.status,
        s.claim_type,
        s.row_count,
        s.max_claimed_amount,
        s.stddev_claimed_amount,
        p.status AS policy_status,
        p.holder_id AS policy_holder_id,
        po.holder_name
    FROM summary s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
)
SELECT
    policy_id,
    status,
    claim_type,
    row_count,
    max_claimed_amount,
    stddev_claimed_amount,
    policy_status,
    policy_holder_id,
    CONCAT_WS(' / ', status, holder_name) AS status_holder_name_label
FROM enriched
ORDER BY claim_type, policy_id, status
LIMIT 10
