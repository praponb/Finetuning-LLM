WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix,
        CONCAT_WS('|', claim_type, status) AS claim_type_status_label,
        CASE WHEN status IN ('PAID', 'OPEN') THEN 'group_a' WHEN status = 'REJECTED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
    WHERE policy_id <= 22
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        reported_at,
        claim_type_status_label,
        COUNT(*) OVER w_adjuster_id_status AS rows_in_adjuster_id_status,
        MAX(approved_amount) OVER (ORDER BY reported_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_max_3
    FROM valid
    WINDOW w_adjuster_id_status AS (PARTITION BY adjuster_id, status)
),
with_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.reported_at,
        s.claim_type_status_label,
        s.rows_in_adjuster_id_status,
        s.approved_amount_max_3,
        p.premium AS policy_premium,
        po.region AS holder_region,
        po.risk_score AS holder_risk_score
    FROM with_trends s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_details_policy s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
        )
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    reported_at,
    claim_type_status_label,
    rows_in_adjuster_id_status,
    approved_amount_max_3,
    holder_region,
    holder_risk_score,
    COALESCE(approved_amount, 0.0) AS approved_amount_filled,
    DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label
FROM screened
ORDER BY id, holder_region DESC NULLS FIRST
