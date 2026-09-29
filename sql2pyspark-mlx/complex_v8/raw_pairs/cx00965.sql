WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        CONCAT_WS('-', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE approved_amount <= 14993.0
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        claim_type_or_unknown,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_3,
        LEAD(claimed_amount, 3, 0) OVER (PARTITION BY adjuster_id ORDER BY reported_at, id) AS next3_claimed_amount,
        LEAD(reported_at) OVER (PARTITION BY policy_id, TO_DATE(reported_at) ORDER BY reported_at, id) AS next_reported_at,
        claimed_amount - LAG(claimed_amount) OVER w_adjuster_id_by_claimed_amoun_2 AS claimed_amount_change,
        ROUND(AVG(claimed_amount) OVER w_adjuster_id, 2) AS adjuster_id_avg_claimed_amount
    FROM recent_claims
    WINDOW w_adjuster_id_by_claimed_amoun_2 AS (PARTITION BY adjuster_id ORDER BY claimed_amount DESC, id), w_adjuster_id AS (PARTITION BY adjuster_id)
)
-- Subtotals with ROLLUP
SELECT
    policy_id,
    adjuster_id,
    status,
    COUNT(*) AS n_rows
FROM metrics
GROUP BY ROLLUP(policy_id, adjuster_id, status)
ORDER BY policy_id NULLS LAST, adjuster_id NULLS LAST, status NULLS LAST
