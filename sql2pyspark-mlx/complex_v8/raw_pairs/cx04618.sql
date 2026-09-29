WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM claims
    WHERE claim_date IS NOT NULL
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.status,
        s.claim_type_len,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id,
        c.reported_at AS claim_reported_at
    FROM base_claims s
    JOIN claims c ON c.policy_id = s.policy_id AND HOUR(c.reported_at) BETWEEN 9 AND 11
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type_len,
        s.claim_id,
        s.claim_status,
        s.claim_adjuster_id,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product,
        p.end_date AS policy_end_date
    FROM exploded_detail_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
joined_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type_len,
        s.claim_id,
        s.claim_adjuster_id,
        s.policy_holder_id,
        s.policy_product,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM with_parent_policy s
    JOIN adjusters a ON a.id = s.adjuster_id
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM joined_adjuster
    WHERE adjuster_level > (SELECT AVG(adjuster_level) FROM joined_adjuster)
),
latest_per_key AS (
    -- Keep the latest row per policy_product, claim_adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type_len,
        claim_id,
        claim_adjuster_id,
        policy_holder_id,
        policy_product,
        adjuster_level,
        adjuster_name,
        adjuster_team
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_product, claim_adjuster_id ORDER BY adjuster_level DESC, id DESC, claim_id DESC) AS recency_rank
        FROM qualified
    ) ranked
    WHERE recency_rank = 1
),
joined_policy AS (
    -- Attach policy attributes
    SELECT
        s.*,
        p.premium AS policy_premium,
        p.status AS policy_status,
        p.holder_id AS policy_holder_id_2
    FROM latest_per_key s
    LEFT JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type_len,
    claim_id,
    claim_adjuster_id,
    policy_holder_id,
    policy_product,
    adjuster_level,
    adjuster_name,
    adjuster_team,
    policy_premium,
    policy_status
FROM joined_policy
ORDER BY adjuster_team DESC, id, claim_id
LIMIT 20
