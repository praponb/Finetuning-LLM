WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        reported_at,
        CASE WHEN claimed_amount >= 28900.9 THEN 'A' WHEN claimed_amount >= 15381.6 THEN 'B' ELSE 'C' END AS claimed_amount_tier,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE approved_amount > 0.0
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_all_by_approved_amount) AS days_since_prev_claim_date,
        NTILE(4) OVER (ORDER BY claim_date DESC, id) AS claim_date_quartile,
        SIZE(COLLECT_SET(adjuster_id) OVER w_policy_id) AS distinct_adjuster_id_in_policy_id,
        LAG(approved_amount, 1, 0) OVER (PARTITION BY adjuster_id ORDER BY approved_amount DESC, id) AS prev_approved_amount,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY adjuster_id ORDER BY approved_amount DESC, id) AS approved_amount_change,
        LAG(reported_at, 2) OVER w_policy_id_by_approved_amount AS prev2_reported_at,
        MIN(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_min_approved_amount,
        COUNT(*) OVER (PARTITION BY adjuster_id, YEAR(reported_at)) AS rows_in_adjuster_id_reported_at_year,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY policy_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        ROW_NUMBER() OVER w_all_by_approved_amount AS approved_amount_seq,
        SIZE(COLLECT_SET(status_or_unknown) OVER (PARTITION BY adjuster_id, YEAR(reported_at))) AS distinct_status_or_unknown_in_adjuster_id_report,
        ROUND(approved_amount - AVG(approved_amount) OVER w_adjuster_id_reported_at_year, 2) AS approved_amount_vs_adjuster_id_reported_at_year_
    FROM recent
    WINDOW w_all_by_approved_amount AS (ORDER BY approved_amount, id), w_policy_id AS (PARTITION BY policy_id), w_policy_id_by_approved_amount AS (PARTITION BY policy_id ORDER BY approved_amount, id), w_adjuster_id_reported_at_year AS (PARTITION BY adjuster_id, YEAR(reported_at))
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_date,
        approved_amount,
        reported_at,
        claimed_amount_tier,
        days_since_prev_claim_date,
        claim_date_quartile,
        distinct_adjuster_id_in_policy_id,
        approved_amount_change,
        prev2_reported_at,
        rows_in_adjuster_id_reported_at_year,
        second_claim_date,
        approved_amount_seq,
        approved_amount_vs_adjuster_id_reported_at_year_,
        ROUND(prev_approved_amount / NULLIF(SUM(prev_approved_amount) OVER w_distinct_status_or_unknown_i, 0), 4) AS prev_approved_amount_share_of_distinct_status_or,
        LEAD(claim_date_quartile) OVER (PARTITION BY YEAR(reported_at) ORDER BY claim_date, id) AS next_claim_date_quartile,
        SIZE(COLLECT_SET(claimed_amount_tier) OVER (PARTITION BY distinct_status_or_unknown_in_adjuster_id_report, distinct_adjuster_id_in_policy_id)) AS distinct_claimed_amount_tier_in_distinct_status_,
        SUM(claim_date_quartile) OVER (PARTITION BY distinct_status_or_unknown_in_adjuster_id_report, distinct_adjuster_id_in_policy_id) AS distinct_status_or_unknown_in_adjuster_id_report_2,
        ROUND(AVG(prev_approved_amount) OVER (PARTITION BY YEAR(reported_at) ORDER BY claim_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS prev_approved_amount_avg_3_prior,
        ROUND(CUME_DIST() OVER w_reported_at_year_by_claim_da_3, 3) AS claim_date_cume_dist,
        SUM(approved_amount) OVER (PARTITION BY claim_date_quartile, DATE_TRUNC('MONTH', prev2_reported_at) ORDER BY policy_id_min_approved_amount DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_3_prior
    FROM windowed
    WINDOW w_distinct_status_or_unknown_i AS (PARTITION BY distinct_status_or_unknown_in_adjuster_id_report, distinct_adjuster_id_in_policy_id), w_reported_at_year_by_claim_da_3 AS (PARTITION BY YEAR(reported_at) ORDER BY claim_date)
)
-- Final projection
SELECT
    id,
    policy_id,
    approved_amount,
    reported_at,
    days_since_prev_claim_date,
    claim_date_quartile,
    distinct_adjuster_id_in_policy_id,
    approved_amount_change,
    prev2_reported_at,
    rows_in_adjuster_id_reported_at_year,
    second_claim_date,
    approved_amount_seq,
    approved_amount_vs_adjuster_id_reported_at_year_,
    prev_approved_amount_share_of_distinct_status_or,
    distinct_status_or_unknown_in_adjuster_id_report_2,
    prev_approved_amount_avg_3_prior,
    claim_date_cume_dist,
    approved_amount_sum_3_prior,
    DATE_ADD(second_claim_date, 30) AS second_claim_date_plus_30d
FROM sequenced
WHERE prev_approved_amount_share_of_distinct_status_or < 0.6
ORDER BY approved_amount_seq DESC NULLS FIRST
