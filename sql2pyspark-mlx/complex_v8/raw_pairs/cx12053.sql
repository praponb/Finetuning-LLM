WITH recent AS (
    -- Filter claims
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
        ROUND(approved_amount * 0.9, 2) AS approved_amount_adj
    FROM claims
    WHERE claim_type = 'medical'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM recent s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.team NOT IN ('Property', 'Motor')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(reported_at) OVER (ORDER BY approved_amount_adj, id) AS next_reported_at,
        MIN(approved_amount_adj) OVER (PARTITION BY status) AS status_min_approved_amount_adj,
        LAG(claim_date, 2) OVER (ORDER BY approved_amount_adj, id) AS prev2_claim_date,
        ROW_NUMBER() OVER w_claim_type_adjuster_team_by AS rn,
        LAG(claimed_amount) OVER (PARTITION BY claim_type, status ORDER BY approved_amount_adj DESC, id) AS prev_claimed_amount,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), adjuster_level)) AS distinct_claim_type_in_reported_at_month_adjuste,
        DENSE_RANK() OVER (PARTITION BY claim_type, status ORDER BY approved_amount_adj DESC, id) AS approved_amount_adj_dense_rank,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER w_status, 0), 4) AS adjuster_level_share_of_status,
        ROUND(AVG(approved_amount_adj) OVER (ORDER BY approved_amount_adj, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS approved_amount_adj_avg_2
    FROM joined
    WINDOW w_claim_type_adjuster_team_by AS (PARTITION BY claim_type, adjuster_team ORDER BY reported_at ASC, id), w_status AS (PARTITION BY status)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE rn <= 5
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    approved_amount,
    reported_at,
    approved_amount_adj,
    adjuster_name,
    adjuster_team,
    adjuster_level,
    next_reported_at,
    status_min_approved_amount_adj,
    rn,
    prev_claimed_amount,
    DATE_ADD(claim_date, 7) AS claim_date_plus_7d
FROM first_rows
ORDER BY claim_date DESC, id
LIMIT 10
