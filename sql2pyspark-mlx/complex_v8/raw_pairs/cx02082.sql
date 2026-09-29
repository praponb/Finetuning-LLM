WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        CONCAT_WS('|', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE policy_id IS NOT NULL
      AND claim_type = 'medical'
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM cleaned_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claimed_amount >= 6745.1
        )
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, COUNT(*) AS claim_count, COUNT(*) AS claim_count_2
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_count_2, 0) AS claim_count_2
    FROM qualified s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
derived AS (
    -- Label and score rows
    SELECT *, LOWER(TRIM(status)) AS status_clean
    FROM profiled
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND HOUR(c.reported_at) BETWEEN 8 AND 13
              AND c.approved_amount <= 24718.5
        )
),
unique_rows AS (
    -- Keep the earliest row per status
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        claim_type_status_label,
        claim_count,
        claim_count_2,
        status_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY claimed_amount, id) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, claim_count_2, claim_count
    FROM unique_rows
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR field IN (claim_count_2 AS claim_count_2_v, claim_count AS m_claim_count)
) unp
ORDER BY id, field
