WITH valid AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE status IN ('APPROVED', 'IN_REVIEW')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        SIZE(COLLECT_SET(policy_id) OVER w_adjuster_id_status) AS distinct_policy_id_in_adjuster_id_status
    FROM valid
    WINDOW w_adjuster_id_status AS (PARTITION BY adjuster_id, status)
),
deduped AS (
    -- Keep the latest row per status
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        last_known_approved_amount,
        distinct_policy_id_in_adjuster_id_status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY claim_date DESC, id DESC) AS row_num
        FROM with_trends
    ) ranked
    WHERE row_num = 1
),
unique_rows AS (
    -- Keep the latest row per claim_type
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        last_known_approved_amount,
        distinct_policy_id_in_adjuster_id_status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY claim_date DESC, id DESC) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
),
joined AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.last_known_approved_amount,
        s.distinct_policy_id_in_adjuster_id_status,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM unique_rows s
    JOIN adjusters a ON a.id = s.adjuster_id
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MIN(claim_date) AS first_claim_date,
        COUNT(CASE WHEN status IN ('APPROVED', 'PAID') THEN 1 END) AS claim_status_count
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_date,
        COALESCE(c.claim_status_count, 0) AS claim_status_count
    FROM joined s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    claim_type,
    claimed_amount,
    last_known_approved_amount,
    adjuster_team,
    first_claim_date,
    GREATEST(claim_status_count, adjuster_level) AS greatest_claim_status_count_adjuster_level,
    ROUND(claimed_amount * 0.25, 2) AS claimed_amount_adj
FROM with_stats
WHERE adjuster_id IS NOT NULL
ORDER BY claimed_amount_adj, first_claim_date, id
LIMIT 5
