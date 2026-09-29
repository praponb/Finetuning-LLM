WITH scoped_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND claim_type IN ('fire', 'theft', 'delay')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        reported_at,
        RANK() OVER (PARTITION BY policy_id ORDER BY approved_amount DESC) AS approved_amount_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_id ORDER BY claim_date DESC), 3) AS claim_date_cume_dist,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY claim_type), 0), 4) AS approved_amount_share_of_claim_type,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_3,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id), 2) AS approved_amount_vs_claim_date_month_policy_id_av,
        MAX(approved_amount) OVER (PARTITION BY claim_type) AS claim_type_max_approved_amount,
        DENSE_RANK() OVER (PARTITION BY policy_id ORDER BY approved_amount DESC, id) AS approved_amount_dense_rank,
        ROUND(PERCENT_RANK() OVER w_claim_date_year_adjuster_id, 3) AS reported_at_percent_rank,
        COUNT(*) OVER (PARTITION BY YEAR(claim_date), adjuster_id ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(PERCENT_RANK() OVER w_adjuster_id_by_reported_at_t, 3) AS reported_at_percent_rank_2,
        LAST_VALUE(claim_date) OVER (PARTITION BY claim_type ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        SUM(approved_amount) OVER (PARTITION BY claim_type) AS claim_type_sum_approved_amount
    FROM scoped_claims
    WINDOW w_claim_date_year_adjuster_id AS (PARTITION BY YEAR(claim_date), adjuster_id ORDER BY reported_at), w_adjuster_id_by_reported_at_t AS (PARTITION BY adjuster_id ORDER BY reported_at ASC)
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        reported_at,
        approved_amount_rank,
        claim_date_cume_dist,
        rows_in_claim_type,
        approved_amount_share_of_claim_type,
        approved_amount_avg_3,
        approved_amount_vs_claim_date_month_policy_id_av,
        claim_type_max_approved_amount,
        reported_at_percent_rank,
        running_count,
        reported_at_percent_rank_2,
        last_claim_date,
        claim_type_sum_approved_amount
    FROM windowed
    WHERE approved_amount_dense_rank <= 5
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY adjuster_id, policy_id) AS rows_in_adjuster_id_policy_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id ORDER BY approved_amount_share_of_claim_type DESC), 3) AS approved_amount_share_of_claim_type_percent_rank,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY YEAR(reported_at), approved_amount_rank ORDER BY running_count NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY YEAR(reported_at), approved_amount_rank ORDER BY running_count NULLS LAST, id) AS seq_no,
        FIRST_VALUE(rows_in_claim_type) IGNORE NULLS OVER (PARTITION BY YEAR(reported_at), approved_amount_rank ORDER BY running_count NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rows_in_claim_type
    FROM leaders
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    reported_at,
    approved_amount_rank,
    claim_date_cume_dist,
    approved_amount_share_of_claim_type,
    approved_amount_avg_3,
    approved_amount_vs_claim_date_month_policy_id_av,
    claim_type_max_approved_amount,
    reported_at_percent_rank,
    running_count,
    reported_at_percent_rank_2,
    last_claim_date,
    claim_type_sum_approved_amount,
    rows_in_adjuster_id_policy_id,
    approved_amount_share_of_claim_type_percent_rank,
    last_known_approved_amount,
    DAYOFWEEK(last_claim_date) AS last_claim_date_dayofweek
FROM metrics
ORDER BY last_claim_date_dayofweek DESC NULLS FIRST, reported_at
