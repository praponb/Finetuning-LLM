WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE adjuster_id <= 5
),
tagged AS (
    -- Add calculated columns
    SELECT *, CONCAT_WS('|', claim_type, status) AS claim_type_status_label
    FROM scoped_claims
    WHERE approved_amount <= 23487.5
),
per_group AS (
    -- Group-level totals
    SELECT
        claim_type,
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        policy_id,
        MAX(approved_amount) AS max_approved_amount,
        ROUND(SUM(approved_amount), 2) AS total_approved_amount,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        COUNT(*) AS cnt
    FROM tagged
    WHERE claimed_amount < 38909.4
    GROUP BY claim_type, DATE_TRUNC('MONTH', reported_at), policy_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(cnt, max_approved_amount) AS least_cnt_max_approved_amount,
        max_approved_amount + cnt AS max_approved_amount_plus_cnt,
        DATE_FORMAT(reported_at_month, 'yyyy-MM-dd') AS reported_at_month_label
    FROM per_group
    WHERE reported_at_month < TIMESTAMP '2025-02-09 09:00:00'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        claim_type,
        reported_at_month,
        policy_id,
        max_approved_amount,
        total_approved_amount,
        claim_type_list,
        cnt,
        least_cnt_max_approved_amount,
        reported_at_month_label,
        NTH_VALUE(max_approved_amount_plus_cnt, 2) OVER (PARTITION BY cnt ORDER BY total_approved_amount, claim_type, reported_at_month, policy_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_approved_amount_plus_cnt,
        RANK() OVER w_cnt_claim_type_by_max_approv AS max_approved_amount_plus_cnt_rank,
        LEAD(total_approved_amount) OVER (PARTITION BY cnt ORDER BY total_approved_amount, claim_type, reported_at_month, policy_id) AS next_total_approved_amount,
        NTILE(3) OVER (PARTITION BY cnt, claim_type ORDER BY max_approved_amount_plus_cnt, claim_type, reported_at_month, policy_id) AS max_approved_amount_plus_cnt_tercile
    FROM labeled
    WINDOW w_cnt_claim_type_by_max_approv AS (PARTITION BY cnt, claim_type ORDER BY max_approved_amount_plus_cnt)
)
SELECT
    claim_type,
    reported_at_month,
    policy_id,
    max_approved_amount,
    total_approved_amount,
    claim_type_list,
    least_cnt_max_approved_amount,
    reported_at_month_label,
    second_max_approved_amount_plus_cnt,
    max_approved_amount_plus_cnt_rank,
    next_total_approved_amount,
    DATEDIFF(DATE '2025-12-31', reported_at_month) AS days_since_reported_at_month
FROM sequenced
ORDER BY policy_id DESC
