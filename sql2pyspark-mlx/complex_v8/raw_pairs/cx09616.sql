WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        ROUND(approved_amount * 1.07, 2) AS approved_amount_adj,
        CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE MONTH(reported_at) IN (1, 2)
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, SUM(approved_amount) AS total_claim_approved_amount
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_approved_amount
    FROM recent_claims s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM profiled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.approved_amount > 0.0
        )
      AND s.claim_type_ref IS NOT NULL
),
latest_per_key AS (
    -- Keep the latest row per status
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        approved_amount_adj,
        claim_type_ref,
        total_claim_approved_amount
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY reported_at DESC, id DESC) AS row_num
        FROM with_activity
    ) ranked
    WHERE row_num = 1
)
SELECT *
FROM (
    SELECT policy_id, reported_at, status, approved_amount
    FROM latest_per_key
) src
PIVOT (
    MIN(approved_amount) AS min_v, SUM(approved_amount) AS sum_v
    FOR status IN ('OPEN', 'PAID', 'REJECTED', 'IN_REVIEW')
)
ORDER BY policy_id, reported_at
