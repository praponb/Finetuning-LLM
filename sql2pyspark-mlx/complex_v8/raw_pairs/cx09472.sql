WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-18 13:00:00'
      AND policy_id IS NOT NULL
),
joined_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        s.reported_at,
        s.claim_date_month,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM recent s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(reported_at) OVER (ORDER BY claim_date_month ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        DENSE_RANK() OVER w_all_by_claim_date_month_ties AS claim_date_month_dense_rank,
        ROUND((UNIX_TIMESTAMP(claim_date_month) - UNIX_TIMESTAMP(LAG(claim_date_month) OVER (ORDER BY claim_date_month ASC, id))) / 60, 1) AS minutes_since_prev_claim_date_month,
        NTH_VALUE(reported_at, 2) OVER (ORDER BY claim_date_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        DENSE_RANK() OVER (ORDER BY claim_date_month, id) AS claim_date_month_dense_rank_2,
        RANK() OVER (ORDER BY reported_at ASC) AS reported_at_rank,
        ROUND(CUME_DIST() OVER w_all_by_claim_date_month_ties, 3) AS claim_date_month_cume_dist,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        LEAD(reported_at) OVER (ORDER BY claim_date_month, id) AS next_reported_at,
        ROW_NUMBER() OVER (ORDER BY claim_date_month, id) AS rn,
        ROUND(PERCENT_RANK() OVER w_all_by_reported_at_ties, 3) AS reported_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY claim_date_month ASC), 3) AS claim_date_month_percent_rank,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (ORDER BY claim_date_month, id))) / 60, 1) AS minutes_since_prev_reported_at
    FROM joined_adjuster
    WINDOW w_all_by_claim_date_month_ties AS (ORDER BY claim_date_month), w_all_by_claim_date_month AS (ORDER BY claim_date_month, id), w_all_by_reported_at_ties AS (ORDER BY reported_at ASC)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    reported_at,
    claim_date_month,
    adjuster_name,
    last_reported_at,
    claim_date_month_dense_rank,
    minutes_since_prev_claim_date_month,
    claim_date_month_dense_rank_2,
    reported_at_rank,
    last_known_reported_at,
    next_reported_at,
    rn,
    reported_at_percent_rank,
    claim_date_month_percent_rank,
    YEAR(second_reported_at) AS second_reported_at_year,
    CASE WHEN rn >= 6 THEN 'HIGH' WHEN rn >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS rn_tier
FROM ranked
ORDER BY adjuster_name, rn, id
LIMIT 5
