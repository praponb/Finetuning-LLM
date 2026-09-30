WITH recent AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
    WHERE (adjuster_id IS NOT NULL OR claimed_amount > 18924.4)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount,
        ABS(claimed_amount - 20436.5) AS claimed_amount_distance
    FROM recent
    WHERE claim_date >= DATE '2024-07-08'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(approved_amount) OVER (PARTITION BY claim_type, status ORDER BY claimed_amount_distance, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_max_3,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY adjuster_id)) AS distinct_claim_type_in_adjuster_id,
        ROUND(approved_amount - AVG(approved_amount) OVER w_adjuster_id, 2) AS approved_amount_vs_adjuster_id_avg
    FROM annotated
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    status,
    greatest_approved_amount_claimed_amount,
    approved_amount_max_3,
    distinct_claim_type_in_adjuster_id,
    approved_amount_vs_adjuster_id_avg
FROM with_trends
WHERE claim_type IN ('collision', 'fire', 'delay')
ORDER BY approved_amount
