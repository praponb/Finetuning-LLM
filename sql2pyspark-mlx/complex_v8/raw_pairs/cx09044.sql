WITH prepared AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE TO_DATE(reported_at) < DATE '2025-03-14'
),
with_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.product AS policy_product,
        p.premium AS policy_premium
    FROM prepared s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claim_date,
        claimed_amount,
        adjuster_team,
        adjuster_level,
        policy_product,
        policy_premium,
        ROUND(approved_amount - AVG(approved_amount) OVER w_policy_id_adjuster_level, 2) AS approved_amount_vs_policy_id_adjuster_level_avg,
        RANK() OVER w_policy_id_claim_date_month_b AS adjuster_level_rank,
        LEAD(claimed_amount, 1, 0) OVER (ORDER BY policy_premium, id) AS next_claimed_amount,
        COUNT(*) OVER (PARTITION BY policy_id, adjuster_level) AS rows_in_policy_id_adjuster_level,
        ROUND(adjuster_level - AVG(adjuster_level) OVER w_policy_id, 2) AS adjuster_level_vs_policy_id_avg,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        ROW_NUMBER() OVER w_all_by_approved_amount AS seq_no,
        ROUND(policy_premium / NULLIF(SUM(policy_premium) OVER w_policy_id, 0), 4) AS policy_premium_share_of_policy_id,
        SUM(approved_amount) OVER (ORDER BY adjuster_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount,
        ROUND(CUME_DIST() OVER (ORDER BY claimed_amount, id), 3) AS claimed_amount_cume_dist,
        ROUND(CUME_DIST() OVER w_all_by_approved_amount, 3) AS approved_amount_cume_dist,
        MIN(policy_premium) OVER (ORDER BY adjuster_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_policy_premium_so_far,
        FIRST_VALUE(claim_date) OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        SIZE(COLLECT_SET(adjuster_team) OVER (PARTITION BY policy_id, adjuster_level)) AS distinct_adjuster_team_in_policy_id_adjuster_lev
    FROM with_details_adjuster
    WINDOW w_policy_id_adjuster_level AS (PARTITION BY policy_id, adjuster_level), w_policy_id_claim_date_month_b AS (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date) ORDER BY adjuster_level DESC), w_policy_id AS (PARTITION BY policy_id), w_all_by_approved_amount AS (ORDER BY approved_amount, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE adjuster_level_rank <= 2
      AND adjuster_team = 'Travel'
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claimed_amount,
    adjuster_team,
    adjuster_level,
    policy_product,
    policy_premium,
    approved_amount_vs_policy_id_adjuster_level_avg,
    adjuster_level_rank,
    rows_in_policy_id_adjuster_level,
    adjuster_level_vs_policy_id_avg,
    rows_in_policy_id,
    policy_premium_share_of_policy_id,
    running_approved_amount,
    claimed_amount_cume_dist,
    approved_amount_cume_dist,
    first_claim_date,
    distinct_adjuster_team_in_policy_id_adjuster_lev,
    seq_no + running_approved_amount AS seq_no_plus_running_approved_amount,
    LOWER(REGEXP_REPLACE(TRIM(policy_product), ' ', '_')) AS policy_product_slug
FROM first_rows
ORDER BY claimed_amount, id
LIMIT 20
