WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        reported_at,
        QUARTER(claim_date) AS claim_date_quarter
    FROM claims
    WHERE (claimed_amount > 5774.6 OR claim_type NOT IN ('collision', 'delay'))
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(claim_date_quarter) OVER w_all_by_reported_at AS prev_claim_date_quarter,
        LEAD(claim_date) OVER (PARTITION BY MONTH(reported_at) ORDER BY approved_amount, id) AS next_claim_date,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id), 0), 4) AS approved_amount_share_of_claim_date_month_policy,
        COUNT(*) OVER (PARTITION BY claim_date_quarter, DATE_TRUNC('MONTH', reported_at)) AS rows_in_claim_date_quarter_reported_at_month,
        ROW_NUMBER() OVER w_claim_date_quarter_adjuster AS seq_no,
        MAX(approved_amount) OVER (ORDER BY reported_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS approved_amount_max_7,
        SUM(claim_date_quarter) OVER (PARTITION BY YEAR(claim_date) ORDER BY approved_amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS claim_date_quarter_sum_7,
        ROUND(AVG(claim_date_quarter) OVER (ORDER BY reported_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS claim_date_quarter_avg_6_prior,
        MIN(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id) AS claim_date_month_policy_id_min_approved_amount,
        ROW_NUMBER() OVER w_all_by_reported_at_2 AS reported_at_seq,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_date_quarter, DATE_TRUNC('MONTH', reported_at)), 2) AS approved_amount_vs_claim_date_quarter_reported_a,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_date_quarter, adjuster_id ORDER BY claim_date DESC), 3) AS claim_date_cume_dist,
        ROUND(claim_date_quarter - AVG(claim_date_quarter) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id), 2) AS claim_date_quarter_vs_claim_date_month_policy_id,
        ROUND(claim_date_quarter / NULLIF(SUM(claim_date_quarter) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id), 0), 4) AS claim_date_quarter_share_of_claim_date_month_pol
    FROM scoped
    WINDOW w_all_by_reported_at AS (ORDER BY reported_at, id), w_claim_date_quarter_adjuster AS (PARTITION BY claim_date_quarter, adjuster_id ORDER BY claim_date DESC, id), w_all_by_reported_at_2 AS (ORDER BY reported_at DESC, id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM with_trends
    WHERE reported_at_seq <= 2
      AND claim_date_quarter_avg_6_prior >= 1.4
)
-- Final projection
SELECT
    id,
    claim_date,
    approved_amount,
    reported_at,
    claim_date_quarter,
    prev_claim_date_quarter,
    next_claim_date,
    approved_amount_share_of_claim_date_month_policy,
    seq_no,
    approved_amount_max_7,
    claim_date_quarter_sum_7,
    claim_date_quarter_avg_6_prior,
    claim_date_month_policy_id_min_approved_amount,
    reported_at_seq,
    approved_amount_vs_claim_date_quarter_reported_a,
    claim_date_cume_dist,
    claim_date_quarter_vs_claim_date_month_policy_id,
    claim_date_quarter_share_of_claim_date_month_pol,
    LAST_DAY(claim_date) AS claim_date_month_end,
    COALESCE(claim_date_quarter, 0) AS claim_date_quarter_filled
FROM top_per_group
ORDER BY seq_no NULLS LAST, claim_date_cume_dist DESC NULLS FIRST
