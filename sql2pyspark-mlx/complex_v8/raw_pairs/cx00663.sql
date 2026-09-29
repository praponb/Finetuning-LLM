WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE status = 'APPROVED'
),
combined_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.reported_at,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM cleaned s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        reported_at,
        adjuster_name,
        adjuster_team,
        adjuster_level,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY policy_id, YEAR(reported_at)), 2) AS approved_amount_vs_policy_id_reported_at_year_av,
        MAX(approved_amount) OVER w_policy_id_reported_at_year AS policy_id_reported_at_year_max_approved_amount,
        COUNT(*) OVER (PARTITION BY policy_id, YEAR(reported_at)) AS rows_in_policy_id_reported_at_year,
        LAST_VALUE(reported_at) OVER (PARTITION BY policy_id, adjuster_team ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at
    FROM combined_details
    WINDOW w_policy_id_reported_at_year AS (PARTITION BY policy_id, YEAR(reported_at))
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_id,
        adjuster_level,
        approved_amount_vs_policy_id_reported_at_year_av,
        policy_id_reported_at_year_max_approved_amount,
        rows_in_policy_id_reported_at_year,
        last_reported_at,
        HOUR(last_reported_at) AS last_reported_at_hour,
        ROUND(approved_amount / NULLIF(adjuster_level, 0), 3) AS approved_amount_per_adjuster_level
    FROM with_windows
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_level,
        approved_amount_vs_policy_id_reported_at_year_av,
        policy_id_reported_at_year_max_approved_amount,
        rows_in_policy_id_reported_at_year,
        last_reported_at,
        last_reported_at_hour,
        approved_amount_per_adjuster_level,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER w_last_reported_at_hour, 0), 4) AS adjuster_level_share_of_last_reported_at_hour,
        ROW_NUMBER() OVER w_adjuster_level_last_reported AS rn,
        DENSE_RANK() OVER w_adjuster_level_last_reported_2 AS adjuster_level_dense_rank,
        ROUND(last_reported_at_hour / NULLIF(SUM(last_reported_at_hour) OVER (PARTITION BY last_reported_at_hour), 0), 4) AS last_reported_at_hour_share_of_last_reported_at_
    FROM enriched
    WINDOW w_last_reported_at_hour AS (PARTITION BY last_reported_at_hour), w_adjuster_level_last_reported AS (PARTITION BY adjuster_level, last_reported_at_hour ORDER BY adjuster_level, id), w_adjuster_level_last_reported_2 AS (PARTITION BY adjuster_level, last_reported_at_hour ORDER BY adjuster_level)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_level,
        approved_amount_vs_policy_id_reported_at_year_av,
        rows_in_policy_id_reported_at_year,
        last_reported_at,
        approved_amount_per_adjuster_level,
        rn,
        last_reported_at_hour_share_of_last_reported_at_,
        ROUND(last_reported_at_hour / NULLIF(SUM(last_reported_at_hour) OVER w_rn, 0), 4) AS last_reported_at_hour_share_of_rn,
        FIRST_VALUE(rn) OVER (PARTITION BY adjuster_level_dense_rank ORDER BY adjuster_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rn,
        RANK() OVER (PARTITION BY adjuster_level_dense_rank ORDER BY adjuster_level DESC, id) AS adjuster_level_rank,
        FIRST_VALUE(adjuster_level_dense_rank) IGNORE NULLS OVER (PARTITION BY adjuster_level_dense_rank ORDER BY adjuster_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_adjuster_level_dense_rank
    FROM ranked
    WINDOW w_rn AS (PARTITION BY rn)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM sequenced
    WHERE first_rn < (SELECT MAX(first_rn) FROM sequenced)
)
-- Final projection
SELECT
    id,
    adjuster_level,
    approved_amount_vs_policy_id_reported_at_year_av,
    rows_in_policy_id_reported_at_year,
    last_reported_at,
    approved_amount_per_adjuster_level,
    rn,
    last_reported_at_hour_share_of_last_reported_at_,
    last_reported_at_hour_share_of_rn,
    first_rn,
    adjuster_level_rank,
    first_known_adjuster_level_dense_rank,
    ABS(adjuster_level - 3) AS adjuster_level_distance,
    DATEDIFF(DATE '2025-06-30', last_reported_at) AS days_since_last_reported_at
FROM qualified
ORDER BY rn DESC NULLS FIRST, adjuster_level DESC
