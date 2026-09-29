WITH recent AS (
    -- Filter claims
    SELECT *
    FROM claims
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        INITCAP(TRIM(status)) AS status_clean
    FROM recent
    WHERE adjuster_id IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        claim_type_len - LAG(claim_type_len) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_type_len NULLS LAST, id) AS claim_type_len_change,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id), 0), 4) AS approved_amount_share_of_policy_id
    FROM tagged
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    approved_amount,
    claim_type_prefix,
    status_clean,
    claim_type_len_change
FROM windowed
ORDER BY adjuster_id DESC
