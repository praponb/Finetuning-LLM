WITH prepared_claims AS (
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
        ROUND(claimed_amount * 1.1, 2) AS claimed_amount_adj,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount
    FROM claims
),
with_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claimed_amount_adj,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM prepared_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.level > 1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        adjuster_level,
        COUNT(*) OVER w_adjuster_team_status AS rows_in_adjuster_team_status,
        ROUND(PERCENT_RANK() OVER w_adjuster_team_by_claimed_amo, 3) AS claimed_amount_adj_percent_rank,
        LEAD(reported_at) OVER w_adjuster_level_by_claimed_am AS next_reported_at,
        ROUND(CUME_DIST() OVER (PARTITION BY status ORDER BY claimed_amount DESC, id), 3) AS claimed_amount_cume_dist,
        claimed_amount_adj - LAG(claimed_amount_adj) OVER (PARTITION BY adjuster_level, adjuster_team ORDER BY adjuster_level, id) AS claimed_amount_adj_change,
        MAX(adjuster_level) OVER (PARTITION BY adjuster_team, status) AS adjuster_team_status_max_adjuster_level,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY claimed_amount DESC, id) AS rn,
        ROUND(CUME_DIST() OVER w_adjuster_level_by_claimed_am, 3) AS claimed_amount_adj_cume_dist,
        MIN(approved_amount) OVER (PARTITION BY adjuster_team, status) AS adjuster_team_status_min_approved_amount
    FROM with_details
    WINDOW w_adjuster_team_status AS (PARTITION BY adjuster_team, status), w_adjuster_team_by_claimed_amo AS (PARTITION BY adjuster_team ORDER BY claimed_amount_adj), w_adjuster_level_by_claimed_am AS (PARTITION BY adjuster_level ORDER BY claimed_amount_adj ASC, id)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        DATE_TRUNC('DAY', reported_at) AS reported_at_day,
        DATE_TRUNC('YEAR', next_reported_at) AS next_reported_at_year
    FROM metrics
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(rn) OVER w_reported_at_month_reported_a AS reported_at_month_reported_at_day_to_date_sum_rn,
        LAG(adjuster_level) OVER (PARTITION BY TO_DATE(next_reported_at_year), adjuster_level ORDER BY claimed_amount_adj_change, id) AS prev_adjuster_level,
        NTILE(5) OVER (PARTITION BY TO_DATE(next_reported_at_year), adjuster_level ORDER BY claimed_amount_adj_change, id) AS claimed_amount_adj_change_quintile,
        LAG(rows_in_adjuster_team_status) OVER (ORDER BY next_reported_at DESC, id) AS prev_rows_in_adjuster_team_status,
        SUM(rows_in_adjuster_team_status) OVER (ORDER BY next_reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rows_in_adjuster_team_status
    FROM derived
    WINDOW w_reported_at_month_reported_a AS (PARTITION BY MONTH(reported_at), TO_DATE(reported_at_day))
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claimed_amount,
    status,
    adjuster_level,
    rows_in_adjuster_team_status,
    claimed_amount_adj_percent_rank,
    next_reported_at,
    claimed_amount_cume_dist,
    claimed_amount_adj_change,
    rn,
    claimed_amount_adj_cume_dist,
    adjuster_team_status_min_approved_amount,
    reported_at_day,
    reported_at_month_reported_at_day_to_date_sum_rn,
    claimed_amount_adj_change_quintile,
    prev_rows_in_adjuster_team_status,
    running_rows_in_adjuster_team_status,
    CASE WHEN status IN ('REJECTED', 'APPROVED') THEN 'group_a' WHEN status = 'OPEN' THEN 'group_b' ELSE 'other' END AS status_group
FROM sequenced
