WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE claim_date >= DATE '2024-06-26'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY reported_at DESC) AS reported_at_dense_rank,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY claim_type, adjuster_id)) AS distinct_policy_id_in_claim_type_adjuster_id
    FROM valid_claims
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        s.reported_at,
        s.distinct_policy_id_in_claim_type_adjuster_id,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.adjuster_id AS claim_adjuster_id
    FROM sequenced s
    JOIN claims c ON c.policy_id = s.policy_id AND MONTH(c.reported_at) IN (2, 3)
),
claim_type_stats AS (
    -- Avg distinct_policy_id_in_claim_type_adjuster_id per claim_type
    SELECT claim_type, AVG(distinct_policy_id_in_claim_type_adjuster_id) AS avg_distinct_policy_id_in_claim_type_adjuster_id
    FROM line_level
    GROUP BY claim_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_distinct_policy_id_in_claim_type_adjuster_id,
        ROUND(s.distinct_policy_id_in_claim_type_adjuster_id - g.avg_distinct_policy_id_in_claim_type_adjuster_id, 2) AS distinct_policy_id_in_claim_type_adjuster_id_min
    FROM line_level s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.distinct_policy_id_in_claim_type_adjuster_id > g.avg_distinct_policy_id_in_claim_type_adjuster_id
),
joined AS (
    -- Join claims
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.distinct_policy_id_in_claim_type_adjuster_id,
        s.claim_id,
        s.claim_adjuster_id,
        s.avg_distinct_policy_id_in_claim_type_adjuster_id,
        s.distinct_policy_id_in_claim_type_adjuster_id_min,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status
    FROM vs_group s
    LEFT JOIN claims c ON c.id = s.claim_id
),
with_details_policy AS (
    -- Join policies
    SELECT s.*, p.end_date AS policy_end_date, c.reported_at AS claim_reported_at
    FROM joined s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN claims c ON c.id = s.claim_id
),
enriched_claim AS (
    -- Attach claim attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.distinct_policy_id_in_claim_type_adjuster_id,
        s.claim_id,
        s.claim_adjuster_id,
        s.avg_distinct_policy_id_in_claim_type_adjuster_id,
        s.distinct_policy_id_in_claim_type_adjuster_id_min,
        s.claim_status,
        s.policy_end_date,
        c.adjuster_id AS claim_adjuster_id_2,
        c.policy_id AS claim_policy_id
    FROM with_details_policy s
    JOIN claims c ON c.id = s.claim_id
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claim_id,
    claim_adjuster_id,
    distinct_policy_id_in_claim_type_adjuster_id_min,
    claim_status,
    policy_end_date,
    claim_adjuster_id_2,
    claim_policy_id,
    CONCAT_WS('-', claim_type, claim_status) AS claim_type_claim_status_label,
    DATE_FORMAT(policy_end_date, 'yyyy-MM-dd') AS policy_end_date_label
FROM enriched_claim
