WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM claims
    WHERE claim_type = 'theft'
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(TRIM(status)) AS status_clean,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM valid_claims
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id, status)) AS distinct_policy_id_in_adjuster_id_status,
        ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY approved_amount_minus_claimed_amount DESC, id) AS approved_amount_minus_claimed_amount_seq
    FROM labeled
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(approved_amount - AVG(approved_amount) OVER w_adjuster_id, 2) AS approved_amount_vs_adjuster_id_avg,
        MIN(approved_amount_filled) OVER (PARTITION BY adjuster_id) AS adjuster_id_min_approved_amount_filled
    FROM metrics
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id)
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_policy_id_in_adjuster_id_status,
    adjuster_id,
    COALESCE(status, 'ALL') AS status,
    GROUPING_ID() AS gid,
    COUNT(*) FILTER (WHERE status IS NOT NULL) AS n_status_is_not_null,
    COUNT(*) AS row_count
FROM with_windows
GROUP BY GROUPING SETS ((distinct_policy_id_in_adjuster_id_status, adjuster_id, status), (distinct_policy_id_in_adjuster_id_status))
ORDER BY distinct_policy_id_in_adjuster_id_status NULLS LAST, adjuster_id NULLS LAST, status NULLS LAST
