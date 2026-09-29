WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref,
        LENGTH(TRIM(status)) AS status_len,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE claim_date < DATE '2025-09-19'
      AND approved_amount >= 0.0
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN status IN ('IN_REVIEW', 'APPROVED') THEN 'group_a' WHEN status = 'OPEN' THEN 'group_b' ELSE 'other' END AS status_group,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        LEAST(status_len, claimed_amount) AS least_status_len_claimed_amount
    FROM valid
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(claimed_amount_bucket) OVER (PARTITION BY adjuster_id) AS adjuster_id_max_claimed_amount_bucket,
        status_len - LAG(status_len) OVER w_policy_id_by_least_status_le AS status_len_change,
        ROUND(approved_amount - AVG(approved_amount) OVER w_adjuster_id, 2) AS approved_amount_vs_adjuster_id_avg,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id,
        NTILE(3) OVER w_policy_id_by_least_status_le AS least_status_len_claimed_amount_tercile,
        RANK() OVER (PARTITION BY policy_id ORDER BY least_status_len_claimed_amount) AS least_status_len_claimed_amount_rank
    FROM scored
    WINDOW w_policy_id_by_least_status_le AS (PARTITION BY policy_id ORDER BY least_status_len_claimed_amount, id), w_adjuster_id AS (PARTITION BY adjuster_id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, approved_amount, least_status_len_claimed_amount
    FROM with_trends
) src
UNPIVOT (
    metric_value FOR metric IN (approved_amount, least_status_len_claimed_amount)
) unp
