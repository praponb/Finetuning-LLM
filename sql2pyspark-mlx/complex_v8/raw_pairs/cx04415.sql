WITH valid_claims AS (
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
        reported_at,
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix,
        CASE WHEN claimed_amount >= 33975.8 THEN 'large' WHEN claimed_amount >= 14019.8 THEN 'medium' ELSE 'small' END AS claimed_amount_tier
    FROM claims
    WHERE status = 'PAID'
      AND claimed_amount >= 4698.6
),
enriched_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_type_prefix,
        s.claimed_amount_tier,
        p.end_date AS policy_end_date
    FROM valid_claims s
    JOIN policies p ON p.id = s.policy_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        claim_type_prefix,
        policy_end_date,
        MAX(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)) AS claim_date_month_max_approved_amount,
        LEAD(approved_amount, 1, 0) OVER (PARTITION BY claimed_amount_tier, adjuster_id ORDER BY claim_date DESC, id) AS next_approved_amount,
        DENSE_RANK() OVER w_all_by_approved_amount_ties AS approved_amount_dense_rank,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY status, YEAR(claim_date))) AS distinct_claim_type_in_status_claim_date_year,
        SIZE(COLLECT_SET(claimed_amount_tier) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date))) AS distinct_claimed_amount_tier_in_claim_date_month,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)), 0), 4) AS approved_amount_share_of_claim_date_month,
        MIN(claimed_amount) OVER (PARTITION BY status, YEAR(claim_date)) AS status_claim_date_year_min_claimed_amount,
        FIRST_VALUE(claim_date) OVER (ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        LAG(claimed_amount, 1, 0) OVER w_all_by_approved_amount AS prev_claimed_amount,
        MAX(approved_amount) OVER (ORDER BY claim_date NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_max_3,
        NTILE(10) OVER (ORDER BY approved_amount DESC, id) AS approved_amount_decile,
        COUNT(*) OVER w_status_claim_date_year AS rows_in_status_claim_date_year
    FROM enriched_policy
    WINDOW w_all_by_approved_amount_ties AS (ORDER BY approved_amount DESC), w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id), w_status_claim_date_year AS (PARTITION BY status, YEAR(claim_date))
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM with_trends
    WHERE approved_amount_dense_rank <= 5
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        status,
        claim_date_month_max_approved_amount,
        approved_amount_dense_rank,
        distinct_claim_type_in_status_claim_date_year,
        distinct_claimed_amount_tier_in_claim_date_month,
        approved_amount_share_of_claim_date_month,
        prev_claimed_amount,
        approved_amount_max_3,
        approved_amount_decile,
        RANK() OVER w_first_claim_date_month_polic AS distinct_claim_type_in_status_claim_date_year_ra,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', first_claim_date), policy_id ORDER BY distinct_claim_type_in_status_claim_date_year DESC), 3) AS distinct_claim_type_in_status_claim_date_year_pe,
        NTH_VALUE(approved_amount_dense_rank, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', first_claim_date), policy_id ORDER BY distinct_claim_type_in_status_claim_date_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount_dense_rank,
        ROUND(AVG(prev_claimed_amount) OVER (PARTITION BY YEAR(policy_end_date), approved_amount_dense_rank), 2) AS policy_end_date_year_approved_amount_dense_rank_,
        SIZE(COLLECT_SET(policy_id) OVER w_policy_end_date_year_approve) AS distinct_policy_id_in_policy_end_date_year_appro,
        LEAD(approved_amount_decile) OVER (PARTITION BY approved_amount_dense_rank, status ORDER BY claim_date_month_max_approved_amount DESC, id) AS next_approved_amount_decile,
        LEAD(next_approved_amount, 1, 0) OVER w_first_claim_date_month_polic_3 AS next_next_approved_amount,
        ROUND(AVG(approved_amount_dense_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', first_claim_date), policy_id ORDER BY distinct_claim_type_in_status_claim_date_year DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS approved_amount_dense_rank_avg_4
    FROM leaders
    WINDOW w_first_claim_date_month_polic AS (PARTITION BY DATE_TRUNC('MONTH', first_claim_date), policy_id ORDER BY distinct_claim_type_in_status_claim_date_year DESC), w_policy_end_date_year_approve AS (PARTITION BY YEAR(policy_end_date), approved_amount_dense_rank), w_first_claim_date_month_polic_3 AS (PARTITION BY DATE_TRUNC('MONTH', first_claim_date), policy_id ORDER BY distinct_claim_type_in_status_claim_date_year DESC, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    status,
    claim_date_month_max_approved_amount,
    distinct_claim_type_in_status_claim_date_year,
    distinct_claimed_amount_tier_in_claim_date_month,
    approved_amount_share_of_claim_date_month,
    prev_claimed_amount,
    distinct_claim_type_in_status_claim_date_year_ra,
    distinct_claim_type_in_status_claim_date_year_pe,
    second_approved_amount_dense_rank,
    policy_end_date_year_approved_amount_dense_rank_,
    distinct_policy_id_in_policy_end_date_year_appro,
    next_approved_amount_decile,
    next_next_approved_amount,
    approved_amount_dense_rank_avg_4,
    CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref
FROM sequenced
WHERE policy_id IS NOT NULL
ORDER BY approved_amount_dense_rank_avg_4
