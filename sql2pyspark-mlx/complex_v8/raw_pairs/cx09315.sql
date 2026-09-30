WITH prepared_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd') AS reported_at_label,
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label_2
    FROM claims
    WHERE status IN ('PAID', 'APPROVED', 'OPEN')
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_status, 2) AS claimed_amount_vs_status_avg,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY status)) AS distinct_policy_id_in_status,
        SUM(claimed_amount) OVER (PARTITION BY status ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount
    FROM prepared_claims
    WINDOW w_status AS (PARTITION BY status)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_claim_type, 2) AS claimed_amount_vs_claim_type_avg,
        ROUND(running_claimed_amount - AVG(running_claimed_amount) OVER (PARTITION BY claim_type), 2) AS running_claimed_amount_vs_claim_type_avg
    FROM with_trends
    WINDOW w_claim_type AS (PARTITION BY claim_type)
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_distinct_policy_id_in_status AS claimed_amount_vs_status_avg_rank,
        ROUND(AVG(running_claimed_amount) OVER (PARTITION BY distinct_policy_id_in_status ORDER BY claimed_amount_vs_status_avg DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS running_claimed_amount_avg_2
    FROM sequenced
    WINDOW w_distinct_policy_id_in_status AS (PARTITION BY distinct_policy_id_in_status ORDER BY claimed_amount_vs_status_avg DESC)
),
with_parent AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.reported_at_label,
        s.reported_at_label_2,
        s.claimed_amount_vs_status_avg,
        s.distinct_policy_id_in_status,
        s.rows_in_claim_type,
        s.claimed_amount_vs_claim_type_avg,
        s.running_claimed_amount_vs_claim_type_avg,
        s.claimed_amount_vs_status_avg_rank,
        s.running_claimed_amount_avg_2,
        p.end_date AS policy_end_date,
        p.product AS policy_product,
        p.start_date AS policy_start_date,
        po.region AS holder_region
    FROM with_trends_2 s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    reported_at_label,
    reported_at_label_2,
    claimed_amount_vs_status_avg,
    distinct_policy_id_in_status,
    rows_in_claim_type,
    running_claimed_amount_vs_claim_type_avg,
    claimed_amount_vs_status_avg_rank,
    running_claimed_amount_avg_2,
    policy_product,
    CASE WHEN rows_in_claim_type >= 13 THEN 'HIGH' WHEN rows_in_claim_type >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS rows_in_claim_type_tier
FROM with_parent
ORDER BY reported_at, running_claimed_amount_vs_claim_type_avg
