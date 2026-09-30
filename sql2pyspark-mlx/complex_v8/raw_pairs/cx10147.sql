WITH prepared AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        DATE_ADD(claim_date, 90) AS claim_date_plus_90d,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek
    FROM claims
    WHERE (claimed_amount >= 18778.6 OR adjuster_id <= 5)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claim_date,
        approved_amount,
        reported_at,
        ROUND(AVG(claim_date_dayofweek) OVER w_claim_date_dayofweek, 2) AS claim_date_dayofweek_avg_claim_date_dayofweek,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_date_dayofweek), 2) AS approved_amount_vs_claim_date_dayofweek_avg,
        COUNT(*) OVER w_claim_date_dayofweek AS rows_in_claim_date_dayofweek,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY claim_date_plus_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        NTILE(10) OVER (ORDER BY claimed_amount DESC NULLS FIRST, id) AS claimed_amount_decile,
        DENSE_RANK() OVER w_all_by_claimed_amount_ties AS claimed_amount_dense_rank,
        COUNT(*) OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(policy_id) OVER w_claim_date_dayofweek) AS distinct_policy_id_in_claim_date_dayofweek,
        ROUND(AVG(claim_date_dayofweek) OVER (PARTITION BY policy_id ORDER BY claim_date_plus_90d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS claim_date_dayofweek_avg_7,
        SUM(claim_date_dayofweek) OVER (PARTITION BY claim_date_dayofweek) AS claim_date_dayofweek_sum_claim_date_dayofweek,
        ROUND(AVG(approved_amount) OVER (ORDER BY claimed_amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_2_prior
    FROM prepared
    WINDOW w_claim_date_dayofweek AS (PARTITION BY claim_date_dayofweek), w_all_by_claimed_amount_ties AS (ORDER BY claimed_amount DESC NULLS FIRST)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_date_dayofweek_avg_claim_date_dayofweek,
        running_count,
        distinct_policy_id_in_claim_date_dayofweek,
        approved_amount_avg_2_prior,
        TO_DATE(last_known_reported_at) AS last_known_reported_at_date,
        FLOOR(claimed_amount_decile / 2) * 2 AS claimed_amount_decile_bucket,
        YEAR(claim_date) AS claim_date_year
    FROM metrics
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_policy_id_in_claim_date_dayofweek, adjuster_id ORDER BY distinct_policy_id_in_claim_date_dayofweek), 3) AS distinct_policy_id_in_claim_date_dayofweek_perce,
        ROUND(claim_date_year - AVG(claim_date_year) OVER (PARTITION BY adjuster_id), 2) AS claim_date_year_vs_adjuster_id_avg,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY claimed_amount_decile_bucket ORDER BY distinct_policy_id_in_claim_date_dayofweek DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        FIRST_VALUE(last_known_reported_at_date) IGNORE NULLS OVER (PARTITION BY claimed_amount_decile_bucket ORDER BY distinct_policy_id_in_claim_date_dayofweek DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_known_reported_at_date,
        ROUND(claim_date_year / NULLIF(SUM(claim_date_year) OVER (PARTITION BY adjuster_id), 0), 4) AS claim_date_year_share_of_adjuster_id
    FROM enriched
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_date_dayofweek_avg_claim_date_dayofweek,
    running_count,
    distinct_policy_id_in_claim_date_dayofweek,
    approved_amount_avg_2_prior,
    last_known_reported_at_date,
    claim_date_year,
    distinct_policy_id_in_claim_date_dayofweek_perce,
    claim_date_year_vs_adjuster_id_avg,
    last_known_claim_date,
    first_known_last_known_reported_at_date
FROM ranked
ORDER BY claim_date_dayofweek_avg_claim_date_dayofweek, approved_amount_avg_2_prior DESC
