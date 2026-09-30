WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE (claim_date >= DATE '2024-08-07' OR claimed_amount < 44921.3)
),
with_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.*,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.end_date AS policy_end_date,
        p.status AS policy_status
    FROM cleaned s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY policy_end_date, id), 3) AS policy_end_date_percent_rank,
        SUM(adjuster_level) OVER (PARTITION BY TO_DATE(reported_at), adjuster_level) AS reported_at_to_date_adjuster_level_sum_adjuster_,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', reported_at)), 2) AS claimed_amount_vs_adjuster_id_reported_at_month_,
        NTILE(4) OVER (PARTITION BY YEAR(policy_end_date), policy_status ORDER BY reported_at DESC, id) AS reported_at_quartile,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY TO_DATE(reported_at), adjuster_level), 2) AS claimed_amount_vs_reported_at_to_date_adjuster_l,
        LAG(policy_end_date) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), adjuster_id ORDER BY claimed_amount ASC, id) AS prev_policy_end_date,
        LAST_VALUE(reported_at) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), adjuster_id ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(policy_end_date), policy_status ORDER BY reported_at DESC), 3) AS reported_at_percent_rank,
        COUNT(*) OVER (PARTITION BY TO_DATE(reported_at), adjuster_level) AS rows_in_reported_at_to_date_adjuster_level,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY TO_DATE(reported_at), adjuster_level), 0), 4) AS adjuster_level_share_of_reported_at_to_date_adju,
        FIRST_VALUE(policy_end_date) IGNORE NULLS OVER (PARTITION BY YEAR(policy_end_date), policy_status ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_policy_end_date,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(policy_end_date), policy_status ORDER BY reported_at DESC, id), 3) AS reported_at_cume_dist,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY TO_DATE(reported_at), adjuster_level)) AS distinct_status_in_reported_at_to_date_adjuster_,
        FIRST_VALUE(claimed_amount) OVER (ORDER BY policy_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount,
        MAX(claimed_amount) OVER (PARTITION BY claim_type ORDER BY adjuster_level DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_1_prior
    FROM with_details_adjuster
    WINDOW w_policy_end_date_year_policy AS (PARTITION BY YEAR(policy_end_date), policy_status ORDER BY reported_at DESC, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(claimed_amount_max_1_prior - AVG(claimed_amount_max_1_prior) OVER (PARTITION BY adjuster_id, reported_at_quartile), 2) AS claimed_amount_max_1_prior_vs_adjuster_id_report,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY distinct_status_in_reported_at_to_date_adjuster_, policy_id)) AS distinct_claim_type_in_distinct_status_in_report,
        MAX(first_claimed_amount) OVER (PARTITION BY distinct_status_in_reported_at_to_date_adjuster_, policy_id) AS distinct_status_in_reported_at_to_date_adjuster__2,
        NTILE(3) OVER (PARTITION BY adjuster_level, claim_type ORDER BY reported_at_quartile DESC NULLS FIRST, id) AS reported_at_quartile_tercile,
        ROUND(reported_at_cume_dist / NULLIF(SUM(reported_at_cume_dist) OVER (PARTITION BY adjuster_id, reported_at_quartile), 0), 4) AS reported_at_cume_dist_share_of_adjuster_id_repor,
        ROUND(AVG(policy_end_date_percent_rank) OVER (PARTITION BY adjuster_level, claim_type ORDER BY reported_at_quartile DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS policy_end_date_percent_rank_avg_6_prior,
        LAG(reported_at_quartile) OVER (PARTITION BY adjuster_level ORDER BY reported_at_quartile, id) AS prev_reported_at_quartile,
        LAG(policy_end_date, 2) OVER (PARTITION BY TO_DATE(reported_at), reported_at_quartile ORDER BY claimed_amount_vs_adjuster_id_reported_at_month_ ASC, id) AS prev2_policy_end_date
    FROM with_trends
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    adjuster_level,
    policy_end_date,
    policy_status,
    policy_end_date_percent_rank,
    claimed_amount_vs_adjuster_id_reported_at_month_,
    reported_at_quartile,
    claimed_amount_vs_reported_at_to_date_adjuster_l,
    prev_policy_end_date,
    reported_at_percent_rank,
    adjuster_level_share_of_reported_at_to_date_adju,
    first_known_policy_end_date,
    reported_at_cume_dist,
    distinct_status_in_reported_at_to_date_adjuster_,
    reported_at_quartile_tercile,
    policy_end_date_percent_rank_avg_6_prior,
    prev_reported_at_quartile,
    prev2_policy_end_date,
    DATE_FORMAT(last_reported_at, 'yyyy-MM-dd HH:00') AS last_reported_at_label,
    adjuster_level - reported_at_percent_rank AS adjuster_level_minus_reported_at_percent_rank
FROM ranked
WHERE status IN ('IN_REVIEW', 'PAID', 'REJECTED')
ORDER BY policy_status DESC NULLS FIRST, status
