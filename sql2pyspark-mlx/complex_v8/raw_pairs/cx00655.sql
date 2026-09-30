WITH scoped AS (
    -- Scope the claims rows
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
        CONCAT_WS(': ', claim_type, status) AS claim_type_status_label,
        INITCAP(TRIM(status)) AS status_clean,
        CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE status = 'PAID'
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        claim_type_status_label,
        status_clean,
        claim_type_ref,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        ABS(claimed_amount - 28426.9) AS claimed_amount_distance,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM scoped
    WHERE claim_type_ref IS NOT NULL
),
rollup_base AS (
    -- Summarise per group
    SELECT
        policy_id,
        claim_type,
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        MAX(reported_at) AS last_reported_at,
        MIN(claimed_amount) AS min_claimed_amount
    FROM derived
    GROUP BY 1, 2, 3
    HAVING MAX(claimed_amount) > 10518.4
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        DATEDIFF(DATE '2026-01-01', reported_at_month) AS days_since_reported_at_month,
        ABS(distinct_adjuster_ids - 4) AS distinct_adjuster_ids_distance
    FROM rollup_base
)
SELECT
    policy_id,
    reported_at_month,
    distinct_adjuster_ids,
    last_reported_at,
    SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix,
    YEAR(reported_at_month) AS reported_at_month_year
FROM enriched
