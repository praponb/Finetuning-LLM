WITH base_claims AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE claim_type NOT IN ('medical', 'collision')
      AND policy_id >= 21
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        p.product AS policy_product
    FROM base_claims s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.product <> 'TRAVEL'
),
policy_product_stats AS (
    -- Max claimed_amount per policy_product
    SELECT policy_product, MAX(claimed_amount) AS max_claimed_amount
    FROM with_parent
    GROUP BY policy_product
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_claimed_amount,
        ROUND(s.claimed_amount - g.max_claimed_amount, 2) AS claimed_amount_minus_max
    FROM with_parent s
    JOIN policy_product_stats g ON g.policy_product = s.policy_product
    WHERE s.claimed_amount = g.max_claimed_amount
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count
    FROM vs_group s
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.policy_product,
        s.max_claimed_amount,
        s.claimed_amount_minus_max,
        p.end_date AS policy_end_date
    FROM summarized_rows s
    JOIN policies p ON p.id = s.policy_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_policy_end_date_year, 0), 4) AS approved_amount_share_of_policy_end_date_year,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY claimed_amount_minus_max DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        RANK() OVER (PARTITION BY adjuster_id ORDER BY claimed_amount_minus_max DESC NULLS FIRST) AS claimed_amount_minus_max_rank,
        NTILE(5) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount_minus_max DESC NULLS FIRST, id) AS claimed_amount_minus_max_quintile
    FROM with_details_policy
    WINDOW w_policy_end_date_year AS (PARTITION BY YEAR(policy_end_date))
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    approved_amount,
    policy_product,
    claimed_amount_minus_max,
    policy_end_date,
    approved_amount_share_of_policy_end_date_year,
    claimed_amount_minus_max_rank,
    claimed_amount_minus_max_quintile,
    CONCAT_WS(' / ', status, policy_product) AS status_policy_product_label
FROM with_windows
ORDER BY adjuster_id
