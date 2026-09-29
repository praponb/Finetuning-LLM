WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        ROUND(approved_amount * 0.25, 2) AS approved_amount_adj
    FROM claims
    WHERE status IS NOT NULL
),
enriched AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.approved_amount_adj,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM valid s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        claim_date,
        claim_type,
        approved_amount_adj,
        adjuster_team,
        adjuster_level,
        ROUND(approved_amount_adj / NULLIF(SUM(approved_amount_adj) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)), 0), 4) AS approved_amount_adj_share_of_claim_date_month,
        SUM(adjuster_level) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)) AS claim_date_month_sum_adjuster_level,
        ROW_NUMBER() OVER (PARTITION BY adjuster_id, adjuster_team ORDER BY adjuster_level DESC, id) AS seq_no,
        NTH_VALUE(adjuster_level, 2) OVER (PARTITION BY adjuster_id, adjuster_team ORDER BY adjuster_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level,
        MIN(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)) AS claim_date_month_min_approved_amount,
        MIN(approved_amount_adj) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)) AS claim_date_month_min_approved_amount_adj,
        SUM(approved_amount_adj) OVER (PARTITION BY adjuster_level ORDER BY adjuster_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount_adj,
        LAG(approved_amount) OVER (PARTITION BY adjuster_level ORDER BY adjuster_level DESC, id) AS prev_approved_amount,
        MIN(adjuster_level) OVER w_claim_date_month AS claim_date_month_min_adjuster_level,
        LAST_VALUE(adjuster_level) OVER (PARTITION BY MONTH(claim_date) ORDER BY adjuster_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_adjuster_level
    FROM enriched
    WINDOW w_claim_date_month AS (PARTITION BY DATE_TRUNC('MONTH', claim_date))
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE seq_no = 1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY adjuster_level, MONTH(claim_date) ORDER BY approved_amount_adj_share_of_claim_date_month) AS approved_amount_adj_share_of_claim_date_month_de,
        MIN(second_adjuster_level) OVER w_seq_no AS seq_no_min_second_adjuster_level,
        SIZE(COLLECT_SET(adjuster_team) OVER (PARTITION BY claim_type, second_adjuster_level)) AS distinct_adjuster_team_in_claim_type_second_adju
    FROM top_per_group
    WINDOW w_seq_no AS (PARTITION BY seq_no)
)
-- Final projection
SELECT DISTINCT running_approved_amount_adj
FROM sequenced
