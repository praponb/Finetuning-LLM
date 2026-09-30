WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        CASE WHEN status IN ('IN_REVIEW', 'PAID') THEN 'group_a' WHEN status = 'APPROVED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
    WHERE claimed_amount < 42447.2
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        status_group,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_status_group_claim_type, 0), 4) AS approved_amount_share_of_status_group_claim_type,
        LAST_VALUE(claim_date) OVER (PARTITION BY adjuster_id ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY status_group, claim_type), 2) AS approved_amount_vs_status_group_claim_type_avg,
        SUM(approved_amount) OVER w_status_group AS status_group_sum_approved_amount,
        NTILE(5) OVER (ORDER BY claim_date DESC, id) AS claim_date_quintile,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY status_group, claim_type)) AS distinct_policy_id_in_status_group_claim_type,
        ROUND(CUME_DIST() OVER w_all_by_claim_date_ties, 3) AS claim_date_cume_dist,
        COUNT(*) OVER w_policy_id AS rows_in_policy_id,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status_group), 2) AS claimed_amount_vs_status_group_avg,
        RANK() OVER w_claim_date_year_by_reported AS reported_at_rank,
        LAST_VALUE(approved_amount) OVER (PARTITION BY YEAR(claim_date) ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount
    FROM recent_claims
    WINDOW w_status_group_claim_type AS (PARTITION BY status_group, claim_type), w_status_group AS (PARTITION BY status_group), w_all_by_claim_date_ties AS (ORDER BY claim_date DESC), w_policy_id AS (PARTITION BY policy_id), w_claim_date_year_by_reported AS (PARTITION BY YEAR(claim_date) ORDER BY reported_at DESC)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status_group,
        last_claim_date,
        claim_date_quintile,
        distinct_policy_id_in_status_group_claim_type,
        claimed_amount_vs_status_group_avg,
        last_approved_amount,
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month
    FROM windowed
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    status_group,
    last_claim_date,
    distinct_policy_id_in_status_group_claim_type,
    claimed_amount_vs_status_group_avg,
    reported_at_month,
    DATE_ADD(claim_date, 90) AS claim_date_plus_90d
FROM enriched
WHERE last_claim_date >= DATE '2024-07-18'
ORDER BY last_claim_date, policy_id DESC NULLS FIRST
