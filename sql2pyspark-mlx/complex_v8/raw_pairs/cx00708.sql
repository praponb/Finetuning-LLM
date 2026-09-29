WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        CASE WHEN claim_type IN ('medical', 'delay') THEN 'group_a' WHEN claim_type = 'theft' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE claim_date IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        claim_type_len,
        claim_type_group,
        NTILE(10) OVER (ORDER BY approved_amount DESC, id) AS approved_amount_decile,
        SUM(claim_type_len) OVER (PARTITION BY claim_type_group) AS claim_type_group_sum_claim_type_len,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER w_all_by_approved_amount)) / 60, 1) AS minutes_since_prev_reported_at
    FROM base_claims
    WINDOW w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id)
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM windowed
    WHERE minutes_since_prev_reported_at > (SELECT MIN(minutes_since_prev_reported_at) FROM windowed)
)
-- Subtotals with CUBE
SELECT
    policy_id,
    approved_amount_decile,
    ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
    ROUND(100 * COUNT(CASE WHEN claim_type <> 'theft' THEN 1 END) / COUNT(*), 1) AS pct_claim_type_theft
FROM filtered
GROUP BY CUBE(policy_id, approved_amount_decile)
ORDER BY policy_id NULLS LAST, approved_amount_decile NULLS LAST
