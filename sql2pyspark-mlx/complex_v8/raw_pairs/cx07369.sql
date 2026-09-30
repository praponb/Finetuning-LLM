WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LOWER(TRIM(claim_type)) AS claim_type_clean,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h,
        CONCAT_WS(' / ', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE approved_amount >= 0.0
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM valid
    WHERE claimed_amount > (SELECT MIN(claimed_amount) FROM valid)
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) AS claim_count,
        MIN(approved_amount) AS min_claim_approved_amount,
        SUM(approved_amount) AS total_claim_approved_amount
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.min_claim_approved_amount,
        c.total_claim_approved_amount
    FROM with_activity s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        claimed_amount,
        status,
        reported_at,
        claim_type_clean,
        reported_at_plus_2h,
        claim_type_status_label,
        claim_count,
        min_claim_approved_amount,
        total_claim_approved_amount,
        LENGTH(TRIM(status)) AS status_len
    FROM with_counts
    WHERE reported_at_plus_2h >= TIMESTAMP '2025-01-09 09:00:00'
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, min_claim_approved_amount, total_claim_approved_amount
    FROM scored
) src
UNPIVOT (
    metric_value FOR metric_name IN (min_claim_approved_amount, total_claim_approved_amount)
) unp
ORDER BY id, metric_name
