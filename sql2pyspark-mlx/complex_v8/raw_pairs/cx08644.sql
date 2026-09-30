WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-25 14:00:00'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        COUNT(*) OVER (PARTITION BY claim_type ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(4) OVER w_claim_type_by_reported_at AS reported_at_quartile,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY status, DATE_TRUNC('MONTH', claim_date))) AS distinct_adjuster_id_in_status_claim_date_month,
        RANK() OVER (ORDER BY claim_date DESC, id) AS claim_date_rank,
        LEAD(claim_date) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), status ORDER BY claim_date, id) AS next_claim_date,
        RANK() OVER (ORDER BY claim_date DESC) AS claim_date_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY reported_at DESC, id), 3) AS reported_at_percent_rank,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id, status), 0), 4) AS approved_amount_share_of_policy_id_status
    FROM base
    WINDOW w_claim_type_by_reported_at AS (PARTITION BY claim_type ORDER BY reported_at DESC, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        running_count,
        reported_at_quartile,
        claim_date_rank,
        next_claim_date,
        claim_date_rank_2,
        reported_at_percent_rank,
        approved_amount_share_of_policy_id_status,
        DATE_TRUNC('QUARTER', next_claim_date) AS next_claim_date_quarter,
        LOWER(TRIM(claim_type)) AS claim_type_clean,
        DATE_TRUNC('YEAR', claim_date) AS claim_date_year
    FROM with_windows
    WHERE distinct_adjuster_id_in_status_claim_date_month < 9
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    status,
    running_count,
    claim_date_rank_2,
    reported_at_percent_rank,
    approved_amount_share_of_policy_id_status,
    next_claim_date_quarter,
    claim_date_year,
    FLOOR(claim_date_rank / 2) * 2 AS claim_date_rank_bucket
FROM labeled
