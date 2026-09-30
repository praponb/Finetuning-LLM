WITH valid AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE claim_type NOT IN ('medical', 'delay')
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_reported_at,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_reported_at
    FROM valid s
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref,
        ABS(approved_amount - 10355.0) AS approved_amount_distance
    FROM summarized_rows
    WHERE claim_date >= DATE '2024-04-07'
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_type = 'flood'
        )
),
with_parent_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.status_ref,
        p.product AS policy_product,
        p.status AS policy_status
    FROM screened s
    JOIN policies p ON p.id = s.policy_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY claimed_amount, id) AS claimed_amount_quartile,
        SUM(claimed_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY policy_product, MONTH(claim_date)), 0), 4) AS claimed_amount_share_of_policy_product_claim_dat,
        LAG(claim_date) OVER (ORDER BY claimed_amount DESC, id) AS prev_claim_date
    FROM with_parent_policy
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    claimed_amount,
    status,
    policy_product,
    policy_status,
    claimed_amount_quartile,
    running_claimed_amount,
    prev_claim_date
FROM with_windows
WHERE claim_type <> 'medical'
ORDER BY claim_date DESC, policy_product
