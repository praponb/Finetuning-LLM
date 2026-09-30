WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
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
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount
    FROM claims
    WHERE claim_type IS NOT NULL
      AND HOUR(reported_at) BETWEEN 10 AND 14
),
with_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.approved_amount_per_claimed_amount,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM recent_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(approved_amount) OVER (PARTITION BY adjuster_team) AS adjuster_team_sum_approved_amount,
        LEAD(adjuster_level) OVER (PARTITION BY adjuster_id, adjuster_level ORDER BY adjuster_level NULLS LAST, id) AS next_adjuster_level,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY adjuster_id, adjuster_level ORDER BY adjuster_level NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        adjuster_level - LAG(adjuster_level) OVER (PARTITION BY adjuster_id, adjuster_level ORDER BY adjuster_level NULLS LAST, id) AS adjuster_level_change
    FROM with_details_adjuster
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(adjuster_level_change) OVER (ORDER BY claim_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS adjuster_level_change_sum_7,
        FIRST_VALUE(approved_amount) OVER (PARTITION BY adjuster_id, adjuster_level ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        DATEDIFF(second_claim_date, LAG(second_claim_date) OVER (ORDER BY claim_date, id)) AS days_since_prev_second_claim_date,
        SUM(claimed_amount) OVER (PARTITION BY adjuster_id, adjuster_level ORDER BY claim_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS claimed_amount_sum_6_prior,
        COUNT(*) OVER (PARTITION BY adjuster_level_change, adjuster_level) AS rows_in_adjuster_level_change_adjuster_level,
        ROUND(PERCENT_RANK() OVER w_next_adjuster_level_by_next, 3) AS next_adjuster_level_percent_rank
    FROM ranked
    WINDOW w_next_adjuster_level_by_next AS (PARTITION BY next_adjuster_level ORDER BY next_adjuster_level DESC)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT second_claim_date, adjuster_team, adjuster_team_sum_approved_amount
    FROM windowed
) src
PIVOT (
    SUM(adjuster_team_sum_approved_amount)
    FOR adjuster_team IN ('Health', 'Travel', 'Property')
)
ORDER BY second_claim_date
