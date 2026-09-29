WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        CASE WHEN claimed_amount >= 38909.4 THEN 'A' WHEN claimed_amount >= 14019.8 THEN 'B' ELSE 'C' END AS claimed_amount_tier
    FROM claims
    WHERE claim_date >= DATE '2024-03-16'
      AND claim_type = 'flood'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        approved_amount,
        LAG(reported_at, 2) OVER w_claim_type_by_reported_at AS prev2_reported_at,
        LAG(approved_amount, 1, 0) OVER (PARTITION BY claimed_amount_tier, adjuster_id ORDER BY approved_amount DESC NULLS FIRST, id) AS prev_approved_amount,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount), 3) AS approved_amount_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY status ORDER BY approved_amount DESC), 3) AS approved_amount_cume_dist_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY reported_at DESC), 3) AS reported_at_percent_rank,
        COUNT(*) OVER (PARTITION BY policy_id, claim_type) AS rows_in_policy_id_claim_type,
        DENSE_RANK() OVER (PARTITION BY claimed_amount_tier, adjuster_id ORDER BY approved_amount DESC NULLS FIRST) AS approved_amount_dense_rank,
        LAG(approved_amount) OVER (PARTITION BY claimed_amount_tier, adjuster_id ORDER BY approved_amount DESC NULLS FIRST, id) AS prev_approved_amount_2,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id, claim_type)) AS distinct_adjuster_id_in_policy_id_claim_type,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY claim_type ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        ROUND(approved_amount - AVG(approved_amount) OVER w_policy_id_claim_type, 2) AS approved_amount_vs_policy_id_claim_type_avg
    FROM cleaned
    WINDOW w_claim_type_by_reported_at AS (PARTITION BY claim_type ORDER BY reported_at DESC, id), w_policy_id_claim_type AS (PARTITION BY policy_id, claim_type)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        approved_amount,
        prev2_reported_at,
        prev_approved_amount,
        approved_amount_cume_dist,
        approved_amount_cume_dist_2,
        reported_at_percent_rank,
        rows_in_policy_id_claim_type,
        approved_amount_dense_rank,
        distinct_adjuster_id_in_policy_id_claim_type,
        approved_amount_vs_policy_id_claim_type_avg,
        QUARTER(prev2_reported_at) AS prev2_reported_at_quarter,
        ABS(second_approved_amount - 4234.9) AS second_approved_amount_distance
    FROM with_windows
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY prev_approved_amount DESC, id), 3) AS prev_approved_amount_cume_dist,
        RANK() OVER (ORDER BY approved_amount_cume_dist_2 DESC, id) AS approved_amount_cume_dist_2_rank,
        NTILE(5) OVER (ORDER BY reported_at_percent_rank ASC, id) AS reported_at_percent_rank_quintile,
        NTILE(4) OVER (PARTITION BY prev2_reported_at_quarter ORDER BY approved_amount, id) AS approved_amount_quartile,
        MIN(prev2_reported_at_quarter) OVER (PARTITION BY prev2_reported_at_quarter, distinct_adjuster_id_in_policy_id_claim_type) AS prev2_reported_at_quarter_distinct_adjuster_id_i,
        RANK() OVER (ORDER BY prev_approved_amount DESC) AS prev_approved_amount_rank
    FROM enriched
)
SELECT
    id,
    approved_amount,
    prev2_reported_at,
    prev_approved_amount,
    rows_in_policy_id_claim_type,
    approved_amount_dense_rank,
    distinct_adjuster_id_in_policy_id_claim_type,
    approved_amount_vs_policy_id_claim_type_avg,
    prev_approved_amount_cume_dist,
    approved_amount_cume_dist_2_rank,
    approved_amount_quartile,
    prev_approved_amount_rank,
    DATE_FORMAT(prev2_reported_at, 'yyyy-MM-dd') AS prev2_reported_at_label,
    DATEDIFF(DATE '2026-01-01', prev2_reported_at) AS days_since_prev2_reported_at
FROM ranked
WHERE second_approved_amount_distance < 40000.0
ORDER BY distinct_adjuster_id_in_policy_id_claim_type, prev_approved_amount_rank DESC NULLS FIRST
