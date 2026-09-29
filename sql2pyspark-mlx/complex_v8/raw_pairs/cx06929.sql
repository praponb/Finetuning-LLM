WITH scoped AS (
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
        CASE WHEN claim_type IN ('theft', 'fire') THEN 'group_a' WHEN claim_type = 'delay' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE claimed_amount >= 7204.1
),
joined AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_type_group,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM scoped s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id AND UPPER(TRIM(a.adjuster_name)) LIKE 'G%'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)), 2) AS claim_date_month_avg_adjuster_level,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_adjuster_id, 0), 4) AS claimed_amount_share_of_adjuster_id,
        MAX(claimed_amount) OVER (PARTITION BY adjuster_team, policy_id ORDER BY adjuster_level DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS claimed_amount_max_4,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id)) AS distinct_policy_id_in_adjuster_id,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY adjuster_team, policy_id ORDER BY adjuster_level DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_3,
        NTILE(10) OVER (PARTITION BY adjuster_team, policy_id ORDER BY adjuster_level DESC, id) AS adjuster_level_decile,
        MIN(adjuster_level) OVER (PARTITION BY claim_type_group ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_adjuster_level_so_far,
        MAX(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)) AS claim_date_month_max_claimed_amount,
        ROUND(CUME_DIST() OVER w_all_by_claim_date_ties, 3) AS claim_date_cume_dist,
        COUNT(*) OVER w_adjuster_id AS rows_in_adjuster_id
    FROM joined
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id), w_all_by_claim_date_ties AS (ORDER BY claim_date DESC)
),
annotated AS (
    -- Add calculated columns
    SELECT *, ABS(claimed_amount - 29359.4) AS claimed_amount_distance
    FROM metrics
)
-- Final projection
SELECT
    id,
    policy_id,
    status,
    adjuster_team,
    adjuster_name,
    adjuster_level,
    claimed_amount_share_of_adjuster_id,
    distinct_policy_id_in_adjuster_id,
    adjuster_level_avg_3,
    adjuster_level_decile,
    min_adjuster_level_so_far,
    claim_date_month_max_claimed_amount,
    claimed_amount_distance,
    DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label,
    claim_date_cume_dist - claimed_amount_distance AS claim_date_cume_dist_minus_claimed_amount_distan
FROM annotated
WHERE adjuster_id <= 3
ORDER BY claim_date_label, adjuster_team
