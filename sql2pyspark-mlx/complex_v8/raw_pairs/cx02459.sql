WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix
    FROM claims
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_reported_at
    FROM filtered s
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        policy_id,
        SUM(claimed_amount) AS total_claimed_amount,
        COUNT(*) FILTER (WHERE claim_type IN ('collision', 'fire', 'medical')) AS n_claim_type_in_collision_fire,
        PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE status = 'OPEN') AS n_status_open
    FROM with_counts
    WHERE claim_type IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', claim_date), policy_id
    HAVING SUM(approved_amount) > 0.0
),
exploded AS (
    -- One row per claim_type_list_item
    SELECT
        s.claim_date_month,
        s.policy_id,
        s.total_claimed_amount,
        s.n_claim_type_in_collision_fire,
        s.median_approved_amount,
        s.distinct_ids,
        s.n_status_open,
        t.claim_type_list_item
    FROM stats s
    LATERAL VIEW EXPLODE(s.claim_type_list) t AS claim_type_list_item
)
-- Final projection
SELECT
    claim_date_month,
    policy_id,
    n_claim_type_in_collision_fire,
    distinct_ids,
    n_status_open,
    claim_type_list_item,
    CONCAT(claim_type_list_item, '#', CAST(policy_id AS STRING)) AS claim_type_list_item_ref
FROM exploded
ORDER BY distinct_ids DESC
