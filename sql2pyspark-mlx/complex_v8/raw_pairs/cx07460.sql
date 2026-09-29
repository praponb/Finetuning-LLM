WITH cleaned_claims AS (
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
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
    FROM claims
    WHERE policy_id IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY status, DATE_TRUNC('MONTH', claim_date_quarter))) AS distinct_claim_type_in_status_claim_date_quarter,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id, adjuster_id), 2) AS policy_id_adjuster_id_avg_approved_amount,
        LAG(claimed_amount, 2) OVER w_policy_id_by_claim_date AS prev2_claimed_amount,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id), 2) AS policy_id_avg_claimed_amount,
        DENSE_RANK() OVER (ORDER BY reported_at) AS reported_at_dense_rank,
        COUNT(*) OVER (PARTITION BY policy_id, adjuster_id) AS rows_in_policy_id_adjuster_id,
        ROW_NUMBER() OVER w_policy_id_adjuster_id_by_app AS approved_amount_seq,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_id, adjuster_id)) AS distinct_claim_type_in_policy_id_adjuster_id,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_policy_id_adjuster_id, 0), 4) AS approved_amount_share_of_policy_id_adjuster_id,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id), 0), 4) AS approved_amount_share_of_policy_id,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY MONTH(claim_date), policy_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at
    FROM cleaned_claims
    WINDOW w_policy_id_by_claim_date AS (PARTITION BY policy_id ORDER BY claim_date, id), w_policy_id_adjuster_id_by_app AS (PARTITION BY policy_id, adjuster_id ORDER BY approved_amount DESC, id), w_policy_id_adjuster_id AS (PARTITION BY policy_id, adjuster_id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY status, claim_type), 2) AS approved_amount_vs_status_claim_type_avg,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY TO_DATE(reported_at), DATE_TRUNC('MONTH', claim_date) ORDER BY distinct_claim_type_in_status_claim_date_quarter NULLS LAST, id)) AS days_since_prev_claim_date,
        ROUND((UNIX_TIMESTAMP(last_known_reported_at) - UNIX_TIMESTAMP(LAG(last_known_reported_at) OVER (PARTITION BY MONTH(reported_at) ORDER BY last_known_reported_at, id))) / 60, 1) AS minutes_since_prev_last_known_reported_at,
        LAST_VALUE(claimed_amount) OVER (ORDER BY distinct_claim_type_in_status_claim_date_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        MAX(reported_at_dense_rank) OVER (PARTITION BY status, claim_type) AS status_claim_type_max_reported_at_dense_rank
    FROM with_windows
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_type,
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        approved_amount_seq,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
    FROM ranked
    WHERE approved_amount_seq >= 2
    GROUP BY claim_type, DATE_TRUNC('MONTH', reported_at), approved_amount_seq
    HAVING MAX(distinct_claim_type_in_policy_id_adjuster_id) > 4
)
-- Final projection
SELECT
    claim_type,
    reported_at_month,
    row_count,
    claim_type_list,
    LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
FROM rollup_base
WHERE SIZE(claim_type_list) >= 1
ORDER BY row_count
