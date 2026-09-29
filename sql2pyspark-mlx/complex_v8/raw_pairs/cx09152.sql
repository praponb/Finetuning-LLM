WITH filtered_claims AS (
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
        reported_at,
        MONTH(claim_date) AS claim_date_month
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per adjuster_id, policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        claim_date_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, policy_id ORDER BY reported_at DESC, id DESC) AS rn
        FROM filtered_claims
    ) ranked
    WHERE rn = 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id ORDER BY claim_date DESC), 3) AS claim_date_cume_dist,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY policy_id ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount
    FROM unique_rows
)
SELECT *
FROM (
    SELECT claim_date_month, policy_id, claim_type, claim_date_cume_dist
    FROM with_trends
) src
PIVOT (
    COUNT(claim_date_cume_dist)
    FOR claim_type IN ('collision', 'medical')
)
ORDER BY claim_date_month, policy_id
