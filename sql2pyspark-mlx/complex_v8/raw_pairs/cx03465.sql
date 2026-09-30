WITH prepared AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        ABS(claimed_amount - 18778.6) AS claimed_amount_distance,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM claims
    WHERE claimed_amount < 29359.4
      AND YEAR(claim_date) = 2024
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        adjuster_id,
        policy_id,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list
    FROM prepared
    WHERE policy_id >= 21
    GROUP BY DATE_TRUNC('MONTH', claim_date), adjuster_id, policy_id
)
SELECT claim_date_month, adjuster_id, metric_name, amount
FROM (
    SELECT claim_date_month, adjuster_id, cnt, distinct_policy_ids
    FROM summary
) src
UNPIVOT (
    amount FOR metric_name IN (cnt, distinct_policy_ids)
) unp
