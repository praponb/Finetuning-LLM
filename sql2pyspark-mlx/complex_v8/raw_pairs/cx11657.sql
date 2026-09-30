WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
filtered AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.team = 'Travel'
              AND a.level BETWEEN 2 AND 3
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(claim_date) OVER w_policy_id_claim_type_by_clai AS prev_claim_date,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_policy_id_claim_type_by_clai) AS days_since_prev_claim_date,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY policy_id, claim_type ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        LAST_VALUE(claim_date) OVER (PARTITION BY policy_id, claim_type ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        FIRST_VALUE(claim_date) OVER (PARTITION BY policy_id, claim_type ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        DENSE_RANK() OVER (PARTITION BY policy_id, claim_type ORDER BY claim_date, id) AS claim_date_dense_rank
    FROM filtered
    WINDOW w_policy_id_claim_type_by_clai AS (PARTITION BY policy_id, claim_type ORDER BY claim_date, id)
),
stats AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        MIN(claim_date_dense_rank) AS min_claim_date_dense_rank,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        MIN(days_since_prev_claim_date) AS min_days_since_prev_claim_date,
        ROUND(100 * COUNT(CASE WHEN claim_type = 'theft' THEN 1 END) / COUNT(*), 1) AS pct_claim_type_theft,
        COUNT(DISTINCT status) AS distinct_status,
        MIN(first_claim_date) AS first_first_claim_date
    FROM metrics
    WHERE status = 'PAID'
    GROUP BY adjuster_id
    HAVING MAX(days_since_prev_claim_date) > 30
),
with_details AS (
    -- Attach adjuster attributes
    SELECT
        s.adjuster_id,
        s.min_claim_date_dense_rank,
        s.claim_type_list,
        s.min_days_since_prev_claim_date,
        s.first_first_claim_date,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM stats s
    JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY adjuster_level), 0), 4) AS adjuster_level_share_of_adjuster_level,
        ROUND(PERCENT_RANK() OVER w_all_by_min_claim_date_dense, 3) AS min_claim_date_dense_rank_percent_rank,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY adjuster_level), 2) AS adjuster_level_vs_adjuster_level_avg,
        MAX(adjuster_level) OVER (PARTITION BY adjuster_level) AS adjuster_level_max_adjuster_level,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY adjuster_level)) AS distinct_adjuster_id_in_adjuster_level,
        LAST_VALUE(first_first_claim_date) IGNORE NULLS OVER (ORDER BY adjuster_level, adjuster_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_first_claim_date
    FROM with_details
    WINDOW w_all_by_min_claim_date_dense AS (ORDER BY min_claim_date_dense_rank ASC, adjuster_id)
),
latest AS (
    -- Keep the earliest row per adjuster_team
    SELECT
        adjuster_id,
        min_claim_date_dense_rank,
        claim_type_list,
        min_days_since_prev_claim_date,
        first_first_claim_date,
        adjuster_name,
        adjuster_level,
        adjuster_team,
        adjuster_level_share_of_adjuster_level,
        min_claim_date_dense_rank_percent_rank,
        adjuster_level_vs_adjuster_level_avg,
        adjuster_level_max_adjuster_level,
        distinct_adjuster_id_in_adjuster_level,
        last_known_first_first_claim_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_team ORDER BY min_days_since_prev_claim_date, adjuster_id) AS recency_rank
        FROM ranked
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    adjuster_id,
    min_claim_date_dense_rank,
    claim_type_list,
    min_days_since_prev_claim_date,
    first_first_claim_date,
    adjuster_name,
    adjuster_level,
    adjuster_team,
    min_claim_date_dense_rank_percent_rank,
    adjuster_level_vs_adjuster_level_avg,
    adjuster_level_max_adjuster_level,
    distinct_adjuster_id_in_adjuster_level,
    last_known_first_first_claim_date,
    CASE WHEN adjuster_level >= 3 THEN 'HIGH' WHEN adjuster_level >= 1 THEN 'MEDIUM' ELSE 'LOW' END AS adjuster_level_tier
FROM latest
ORDER BY last_known_first_first_claim_date DESC
