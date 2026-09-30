WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        CASE WHEN status IN ('OPEN', 'REJECTED') THEN 'group_a' WHEN status = 'APPROVED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status_group,
        LEAD(claim_date) OVER w_all_by_reported_at AS next_claim_date,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at) ORDER BY reported_at DESC, id) AS reported_at_quartile,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_approved_amount,
        SUM(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at) ORDER BY reported_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS claimed_amount_sum_3_prior,
        LAST_VALUE(claim_date) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at) ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date
    FROM cleaned
    WINDOW w_all_by_reported_at AS (ORDER BY reported_at DESC NULLS FIRST, id)
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, adjuster_id_avg_approved_amount, claimed_amount, claimed_amount_sum_3_prior
    FROM metrics
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (adjuster_id_avg_approved_amount, claimed_amount, claimed_amount_sum_3_prior)
) unp
ORDER BY id, metric_name
