WITH filtered AS (
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
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(reported_at, 3) OVER w_adjuster_id_by_claimed_amoun AS prev3_reported_at,
        LAG(claimed_amount, 3, 0) OVER w_adjuster_id_by_reported_at AS prev3_claimed_amount
    FROM filtered
    WINDOW w_adjuster_id_by_claimed_amoun AS (PARTITION BY adjuster_id ORDER BY claimed_amount ASC NULLS LAST, id), w_adjuster_id_by_reported_at AS (PARTITION BY adjuster_id ORDER BY reported_at DESC, id)
),
per_group AS (
    -- Summarise per group
    SELECT
        claim_type,
        PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        COUNT(*) FILTER (WHERE status NOT IN ('OPEN', 'REJECTED')) AS n_status_not_in_open_rejected,
        SUM(prev3_claimed_amount) FILTER (WHERE claim_type IN ('medical', 'theft')) AS prev3_claimed_amount_claim_type_in_medical_theft,
        COUNT(*) AS row_count
    FROM with_trends
    WHERE claimed_amount >= 17759.3
    GROUP BY claim_type
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(row_count - 5) AS row_count_distance,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM per_group
),
annotated_2 AS (
    -- Derive helper columns
    SELECT
        claim_type,
        median_claimed_amount,
        prev3_claimed_amount_claim_type_in_medical_theft,
        row_count,
        row_count_distance,
        claim_type_len,
        INITCAP(TRIM(claim_type)) AS claim_type_clean,
        COALESCE(median_claimed_amount, 0.0) AS median_claimed_amount_filled
    FROM annotated
)
SELECT
    claim_type,
    median_claimed_amount,
    row_count,
    row_count_distance,
    claim_type_clean,
    ROUND(median_claimed_amount * 0.9, 2) AS median_claimed_amount_adj
FROM annotated_2
WHERE median_claimed_amount < 35030.0
ORDER BY claim_type DESC NULLS FIRST, row_count DESC
LIMIT 5
