WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        CASE WHEN claim_type IN ('collision', 'fire') THEN 'group_a' WHEN claim_type = 'theft' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        DATE_TRUNC('DAY', reported_at) AS reported_at_day
    FROM claims
    WHERE claim_type = 'theft'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        reported_at_day,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_6_prior,
        COUNT(*) OVER (PARTITION BY policy_id ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_4,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount ASC, id) AS claimed_amount_change,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY claim_type_group, policy_id)) AS distinct_adjuster_id_in_claim_type_group_policy_
    FROM filtered_claims
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        COALESCE(running_count, 0) AS running_count_filled
    FROM ranked
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(running_count_filled) OVER (PARTITION BY distinct_adjuster_id_in_claim_type_group_policy_, claim_type_len) AS distinct_adjuster_id_in_claim_type_group_policy__2,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_adjuster_id_in_claim_type_group_policy_ ORDER BY claim_date ASC, id), 3) AS claim_date_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_adjuster_id_in_claim_type_group_policy_ ORDER BY claim_date ASC), 3) AS claim_date_percent_rank,
        LAST_VALUE(approved_amount_avg_4) OVER (PARTITION BY distinct_adjuster_id_in_claim_type_group_policy_ ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount_avg_4
    FROM labeled
),
with_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.reported_at_day,
        s.claimed_amount_avg_6_prior,
        s.running_count,
        s.approved_amount_avg_4,
        s.claimed_amount_change,
        s.distinct_adjuster_id_in_claim_type_group_policy_,
        s.claim_type_len,
        s.running_count_filled,
        s.distinct_adjuster_id_in_claim_type_group_policy__2,
        s.claim_date_cume_dist,
        s.claim_date_percent_rank,
        p.end_date AS policy_end_date
    FROM with_windows s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.end_date IS NOT NULL
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        running_count,
        approved_amount_avg_4,
        claim_type_len,
        distinct_adjuster_id_in_claim_type_group_policy__2,
        claim_date_cume_dist,
        claim_date_percent_rank,
        policy_end_date,
        DATE_FORMAT(policy_end_date, 'yyyy-MM') AS policy_end_date_label,
        COALESCE(running_count_filled, 0) AS running_count_filled_filled,
        QUARTER(claim_date) AS claim_date_quarter
    FROM with_details_policy
    WHERE claim_type_len >= 6
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        running_count,
        approved_amount_avg_4,
        claim_type_len,
        claim_date_percent_rank,
        policy_end_date,
        policy_end_date_label,
        CONCAT(policy_end_date_label, '#', CAST(claim_date_quarter AS STRING)) AS policy_end_date_label_ref
    FROM derived
    WHERE policy_end_date_label IS NOT NULL
)
SELECT
    id,
    claimed_amount,
    approved_amount_avg_4,
    claim_date_percent_rank,
    policy_end_date_label,
    policy_end_date_label_ref,
    DATE_TRUNC('MONTH', claim_date) AS claim_date_month
FROM tagged
WHERE policy_end_date_label_ref IS NOT NULL
ORDER BY claimed_amount, policy_end_date_label_ref DESC NULLS FIRST
