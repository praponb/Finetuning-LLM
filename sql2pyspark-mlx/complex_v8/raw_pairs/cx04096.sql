WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE (approved_amount >= 0.0 OR status IN ('REJECTED', 'PAID'))
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        status_len,
        COUNT(*) OVER (PARTITION BY status, policy_id) AS rows_in_status_policy_id,
        LEAD(claim_date, 2) OVER (PARTITION BY adjuster_id ORDER BY reported_at ASC, id) AS next2_claim_date,
        FIRST_VALUE(reported_at) OVER (PARTITION BY policy_id, adjuster_id ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at,
        ROUND(PERCENT_RANK() OVER w_all_by_reported_at_ties, 3) AS reported_at_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY reported_at ASC, id) AS seq_no,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY status, policy_id)) AS distinct_adjuster_id_in_status_policy_id,
        ROUND(status_len / NULLIF(SUM(status_len) OVER w_status_policy_id, 0), 4) AS status_len_share_of_status_policy_id,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        MAX(status_len) OVER (PARTITION BY status, policy_id) AS status_policy_id_max_status_len
    FROM valid_claims
    WINDOW w_all_by_reported_at_ties AS (ORDER BY reported_at), w_status_policy_id AS (PARTITION BY status, policy_id)
),
annotated AS (
    -- Add calculated columns
    SELECT *, ABS(approved_amount - 14377.6) AS approved_amount_distance
    FROM windowed
    WHERE claim_date < DATE '2025-04-25'
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    status,
    reported_at,
    status_len,
    rows_in_status_policy_id,
    first_reported_at,
    reported_at_percent_rank,
    distinct_adjuster_id_in_status_policy_id,
    status_len_share_of_status_policy_id,
    first_known_claim_date,
    status_policy_id_max_status_len,
    approved_amount_distance
FROM annotated
WHERE reported_at < TIMESTAMP '2025-02-24 13:00:00'
ORDER BY adjuster_id DESC NULLS FIRST, id DESC
LIMIT 20
