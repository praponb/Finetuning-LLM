WITH cleaned_claims AS (
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
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM claims
    WHERE adjuster_id >= 2
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS approved_amount_max_4,
        SIZE(COLLECT_SET(policy_id) OVER w_status_claim_date_month) AS distinct_policy_id_in_status_claim_date_month,
        FIRST_VALUE(approved_amount_filled) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount_filled
    FROM cleaned_claims
    WINDOW w_status_claim_date_month AS (PARTITION BY status, DATE_TRUNC('MONTH', claim_date))
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix,
        GREATEST(approved_amount_filled, approved_amount_max_4) AS greatest_approved_amount_filled_approved_amount_,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM metrics
    WHERE approved_amount_max_4 BETWEEN 30000.0 AND 97500.0
)
SELECT
    adjuster_id,
    distinct_policy_id_in_status_claim_date_month,
    status,
    GROUPING(adjuster_id) AS is_total_adjuster_id,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT policy_id) AS distinct_policy_ids,
    COUNT(DISTINCT claim_type) AS distinct_claim_types
FROM scored
GROUP BY GROUPING SETS ((adjuster_id, distinct_policy_id_in_status_claim_date_month, status), (adjuster_id), ())
ORDER BY adjuster_id NULLS LAST, distinct_policy_id_in_status_claim_date_month NULLS LAST, status NULLS LAST
