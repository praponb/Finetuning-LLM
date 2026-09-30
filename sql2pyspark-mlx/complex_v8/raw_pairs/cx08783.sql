WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-18 09:00:00'
),
enriched_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        p.start_date AS policy_start_date,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product
    FROM valid s
    JOIN policies p ON p.id = s.policy_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_product, policy_holder_id ORDER BY claim_date), 3) AS claim_date_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_product, policy_holder_id ORDER BY claim_date), 3) AS claim_date_percent_rank,
        SIZE(COLLECT_SET(policy_product) OVER (PARTITION BY policy_id, policy_holder_id)) AS distinct_policy_product_in_policy_id_policy_hold
    FROM enriched_policy
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_date
    FROM metrics s
),
joined_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.policy_start_date,
        s.policy_holder_id,
        s.policy_product,
        s.claim_date_cume_dist,
        s.claim_date_percent_rank,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM summarized_rows s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
combined_details AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.policy_start_date,
        s.policy_holder_id,
        s.policy_product,
        s.claim_date_cume_dist,
        s.claim_date_percent_rank,
        s.adjuster_name,
        a.adjuster_name AS adjuster_adjuster_name,
        a.level AS adjuster_level_2,
        a.team AS adjuster_team,
        p.status AS policy_status
    FROM joined_adjuster s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_details AS (
    -- Join adjusters
    SELECT s.*, a.adjuster_name AS adjuster_adjuster_name_2
    FROM combined_details s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_date,
    claimed_amount,
    policy_holder_id,
    policy_product,
    claim_date_percent_rank,
    adjuster_adjuster_name,
    adjuster_level_2,
    adjuster_team,
    policy_status
FROM with_details
WHERE UPPER(TRIM(adjuster_name)) LIKE 'H%'
ORDER BY claim_date_percent_rank, adjuster_level_2, id
LIMIT 20
