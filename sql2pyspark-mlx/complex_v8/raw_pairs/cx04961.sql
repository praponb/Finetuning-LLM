WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at
    FROM claims
    WHERE approved_amount >= 0.0
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        reported_at,
        LEAD(reported_at, 3) OVER (PARTITION BY MONTH(claim_date), policy_id ORDER BY claim_date DESC, id) AS next3_reported_at,
        LEAD(claim_date) OVER w_all_by_reported_at AS next_claim_date,
        RANK() OVER w_claim_type_by_claim_date AS claim_date_rank,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY MONTH(claim_date), policy_id ORDER BY claim_date DESC, id))) / 60, 1) AS minutes_since_prev_reported_at,
        ROUND(PERCENT_RANK() OVER w_claim_type_policy_id_by_clai, 3) AS claim_date_percent_rank,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY YEAR(claim_date), adjuster_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        LAG(reported_at, 2) OVER (PARTITION BY claim_type ORDER BY claim_date DESC, id) AS prev2_reported_at,
        ROW_NUMBER() OVER w_claim_type_policy_id_by_clai AS claim_date_seq,
        LAST_VALUE(claim_date) OVER (PARTITION BY MONTH(claim_date), policy_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date
    FROM cleaned
    WINDOW w_all_by_reported_at AS (ORDER BY reported_at ASC, id), w_claim_type_by_claim_date AS (PARTITION BY claim_type ORDER BY claim_date DESC, id), w_claim_type_policy_id_by_clai AS (PARTITION BY claim_type, policy_id ORDER BY claim_date DESC, id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref,
        prev2_reported_at + INTERVAL 15 MINUTES AS prev2_reported_at_plus_15m,
        ABS(claim_date_percent_rank - 0.5) AS claim_date_percent_rank_distance
    FROM with_windows
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        claim_date,
        reported_at,
        next3_reported_at,
        next_claim_date,
        minutes_since_prev_reported_at,
        claim_date_percent_rank,
        second_claim_date,
        prev2_reported_at,
        claim_date_seq,
        last_claim_date,
        claim_type_ref,
        prev2_reported_at_plus_15m,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id ORDER BY claim_date), 3) AS claim_date_percent_rank_2,
        MIN(claim_date_seq) OVER w_policy_id AS policy_id_min_claim_date_seq,
        ROUND(claim_date_percent_rank - AVG(claim_date_percent_rank) OVER (PARTITION BY policy_id), 2) AS claim_date_percent_rank_vs_policy_id_avg,
        ROUND(claim_date_percent_rank / NULLIF(SUM(claim_date_percent_rank) OVER (PARTITION BY policy_id), 0), 4) AS claim_date_percent_rank_share_of_policy_id
    FROM labeled
    WINDOW w_policy_id AS (PARTITION BY policy_id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', last_claim_date) AS last_claim_date_month,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        DATE_TRUNC('MONTH', prev2_reported_at_plus_15m) AS prev2_reported_at_plus_15m_month,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(policy_id_min_claim_date_seq) / NULLIF(SUM(claim_date_percent_rank_vs_policy_id_avg), 0), 3) AS policy_id_min_claim_date_seq_to_claim_date_perce,
        MIN(claim_date_seq) AS min_claim_date_seq
    FROM metrics
    WHERE claim_type_ref IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', last_claim_date), DATE_FORMAT(claim_date, 'yyyy-MM'), DATE_TRUNC('MONTH', prev2_reported_at_plus_15m)
    HAVING SUM(claim_date_percent_rank_vs_policy_id_avg) > -0.7
)
-- Final projection
SELECT
    last_claim_date_month,
    prev2_reported_at_plus_15m_month,
    n_rows,
    distinct_ids,
    policy_id_min_claim_date_seq_to_claim_date_perce,
    min_claim_date_seq
FROM rollup_base
ORDER BY prev2_reported_at_plus_15m_month DESC, distinct_ids NULLS LAST
