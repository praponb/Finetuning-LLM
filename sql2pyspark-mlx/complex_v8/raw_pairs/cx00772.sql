WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        MONTH(claim_date) AS claim_date_month,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE status <> 'PAID'
      AND claim_date BETWEEN DATE '2024-03-15' AND DATE '2025-04-20'
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, p.start_date AS policy_start_date, a.level AS adjuster_level
    FROM valid s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_date_month,
        least_approved_amount_claimed_amount,
        policy_start_date,
        adjuster_level,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY adjuster_id, MONTH(claim_date) ORDER BY policy_start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_3,
        RANK() OVER w_adjuster_id_claim_date_month_2 AS policy_start_date_rank,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY adjuster_id, MONTH(claim_date) ORDER BY policy_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        ROUND(claim_date_month / NULLIF(SUM(claim_date_month) OVER w_policy_id_claim_date_month, 0), 4) AS claim_date_month_share_of_policy_id_claim_date_m,
        FIRST_VALUE(adjuster_level) OVER (PARTITION BY adjuster_level ORDER BY policy_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_adjuster_level,
        SIZE(COLLECT_SET(adjuster_id) OVER w_policy_id_claim_date_month) AS distinct_adjuster_id_in_policy_id_claim_date_mon,
        NTH_VALUE(policy_start_date, 2) OVER (ORDER BY least_approved_amount_claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_policy_start_date,
        ROUND(AVG(claim_date_month) OVER (ORDER BY least_approved_amount_claimed_amount DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS claim_date_month_avg_3,
        LAG(claim_date) OVER (ORDER BY least_approved_amount_claimed_amount DESC, id) AS prev_claim_date,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY policy_id, claim_date_month), 0), 4) AS claimed_amount_share_of_policy_id_claim_date_mon,
        MIN(adjuster_level) OVER w_policy_id_claim_date_month AS policy_id_claim_date_month_min_adjuster_level
    FROM joined
    WINDOW w_adjuster_id_claim_date_month_2 AS (PARTITION BY adjuster_id, MONTH(claim_date) ORDER BY policy_start_date, id), w_policy_id_claim_date_month AS (PARTITION BY policy_id, claim_date_month)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claim_date_month,
        policy_start_date,
        adjuster_level,
        adjuster_level_avg_3,
        policy_start_date_rank,
        first_known_reported_at,
        first_adjuster_level,
        distinct_adjuster_id_in_policy_id_claim_date_mon,
        second_policy_start_date,
        prev_claim_date,
        claimed_amount_share_of_policy_id_claim_date_mon,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY YEAR(policy_start_date), claim_date_month), 0), 4) AS adjuster_level_share_of_policy_start_date_year_c,
        LEAD(claim_date_month) OVER (PARTITION BY policy_start_date_rank ORDER BY distinct_adjuster_id_in_policy_id_claim_date_mon DESC, id) AS next_claim_date_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_start_date_rank ORDER BY distinct_adjuster_id_in_policy_id_claim_date_mon DESC), 3) AS distinct_adjuster_id_in_policy_id_claim_date_mon_2,
        SUM(first_adjuster_level) OVER (PARTITION BY YEAR(policy_start_date), claim_date_month) AS policy_start_date_year_claim_date_month_sum_firs,
        COUNT(*) OVER w_policy_start_date_year_claim AS rows_in_policy_start_date_year_claim_date_month,
        adjuster_level - LAG(adjuster_level) OVER w_first_adjuster_level_adjuste AS adjuster_level_change,
        MAX(claim_date_month) OVER (PARTITION BY policy_start_date_rank ORDER BY distinct_adjuster_id_in_policy_id_claim_date_mon DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS claim_date_month_max_3_prior
    FROM sequenced
    WINDOW w_policy_start_date_year_claim AS (PARTITION BY YEAR(policy_start_date), claim_date_month), w_first_adjuster_level_adjuste AS (PARTITION BY first_adjuster_level, adjuster_id ORDER BY claimed_amount_share_of_policy_id_claim_date_mon, id)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_date_month,
    policy_start_date,
    adjuster_level,
    adjuster_level_avg_3,
    policy_start_date_rank,
    first_known_reported_at,
    first_adjuster_level,
    distinct_adjuster_id_in_policy_id_claim_date_mon,
    second_policy_start_date,
    prev_claim_date,
    adjuster_level_share_of_policy_start_date_year_c,
    next_claim_date_month,
    distinct_adjuster_id_in_policy_id_claim_date_mon_2,
    rows_in_policy_start_date_year_claim_date_month,
    claim_date_month_max_3_prior
FROM metrics
WHERE next_claim_date_month >= 5
ORDER BY policy_start_date DESC, first_adjuster_level DESC
