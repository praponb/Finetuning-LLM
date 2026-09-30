WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        INITCAP(TRIM(claim_type)) AS claim_type_clean,
        CONCAT_WS('|', status, claim_type) AS status_claim_type_label,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount
    FROM claims
    WHERE claim_date IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        reported_at,
        claim_type_clean,
        status_claim_type_label,
        approved_amount_plus_claimed_amount,
        COUNT(*) OVER (PARTITION BY status, claim_type) AS rows_in_status_claim_type,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY status, claim_type), 2) AS approved_amount_vs_status_claim_type_avg,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY policy_id, status ORDER BY reported_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY status, claim_type), 0), 4) AS approved_amount_share_of_status_claim_type,
        MAX(approved_amount) OVER (PARTITION BY status, claim_type) AS status_claim_type_max_approved_amount
    FROM base_claims
    WINDOW w_status_claim_type AS (PARTITION BY status, claim_type)
),
tagged AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(claim_type_clean)) AS claim_type_clean_len
    FROM sequenced
)
SELECT id, metric_name, reading
FROM (
    SELECT id, rows_in_status_claim_type, claim_type_clean_len
    FROM tagged
) src
UNPIVOT (
    reading FOR metric_name IN (rows_in_status_claim_type, claim_type_clean_len)
) unp
