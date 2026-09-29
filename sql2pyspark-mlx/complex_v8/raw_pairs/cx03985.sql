WITH base_claims AS (
    -- Keep valid rows and derive helper columns
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
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE claimed_amount > 5313.8
      AND approved_amount <= 32878.5
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        claim_type_prefix,
        status_len,
        COUNT(*) OVER (PARTITION BY status_or_unknown) AS rows_in_status_or_unknown,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY status_len ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount
    FROM base_claims
),
summary AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        COUNT(*) FILTER (WHERE status IS NOT NULL) AS n_status_is_not_null,
        ROUND(AVG(rows_in_status_or_unknown), 2) AS avg_rows_in_status_or_unknown,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        SUM(status_len) FILTER (WHERE status NOT IN ('APPROVED', 'REJECTED')) AS status_len_status_not_in_approved_rejec,
        COUNT(*) FILTER (WHERE claim_type IN ('flood', 'delay', 'collision')) AS n_claim_type_in_flood_delay_co
    FROM sequenced
    GROUP BY 1
    HAVING MAX(rows_in_status_or_unknown) > 9
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, SUM(claimed_amount) AS total_claim_claimed_amount
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_claimed_amount
    FROM summary s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
SELECT
    adjuster_id,
    claim_type_list,
    status_len_status_not_in_approved_rejec,
    total_claim_claimed_amount
FROM profiled
ORDER BY adjuster_id DESC, total_claim_claimed_amount NULLS LAST
LIMIT 10
