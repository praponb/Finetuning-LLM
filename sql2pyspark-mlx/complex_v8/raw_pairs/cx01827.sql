WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        CONCAT_WS(' / ', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE claim_type IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        approved_amount_filled,
        claim_type_status_label,
        RANK() OVER (PARTITION BY status, MONTH(reported_at) ORDER BY reported_at ASC) AS reported_at_rank,
        COUNT(*) OVER (PARTITION BY status, policy_id) AS rows_in_status_policy_id,
        SUM(claimed_amount) OVER (PARTITION BY status, policy_id) AS status_policy_id_sum_claimed_amount
    FROM valid
),
combined_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.approved_amount_filled,
        s.reported_at_rank,
        s.rows_in_status_policy_id,
        s.status_policy_id_sum_claimed_amount,
        p.end_date AS policy_end_date,
        p.status AS policy_status,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM ranked s
    JOIN policies p ON p.id = s.policy_id AND p.product <> 'AUTO'
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    WHERE p.end_date IS NOT NULL
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        adjuster_id,
        adjuster_level,
        SUM(adjuster_level) AS total_adjuster_level,
        ARRAY_SORT(COLLECT_SET(adjuster_team)) AS adjuster_team_list
    FROM combined_details_policy
    GROUP BY 1, 2, 3
)
SELECT
    claim_date_month,
    adjuster_level,
    total_adjuster_level,
    adjuster_team_list
FROM totals
ORDER BY claim_date_month DESC NULLS FIRST, adjuster_level NULLS LAST
