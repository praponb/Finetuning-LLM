WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE status = 'PAID'
      AND adjuster_id <= 7
),
joined_adjuster AS (
    -- Enrich with dimension data
    SELECT s.*, a.team AS adjuster_team
    FROM filtered_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        SIZE(COLLECT_SET(adjuster_id) OVER w_status_adjuster_team) AS distinct_adjuster_id_in_status_adjuster_team,
        COUNT(*) OVER (PARTITION BY status, adjuster_team) AS rows_in_status_adjuster_team,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type, policy_id ORDER BY claim_date ASC, id), 3) AS claim_date_cume_dist,
        ROUND(approved_amount - AVG(approved_amount) OVER w_status_adjuster_team, 2) AS approved_amount_vs_status_adjuster_team_avg,
        SUM(approved_amount) OVER (PARTITION BY status, adjuster_team) AS status_adjuster_team_sum_approved_amount,
        SUM(claimed_amount) OVER (ORDER BY claim_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY claim_type, policy_id ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY status, adjuster_team)) AS distinct_policy_id_in_status_adjuster_team,
        SUM(claimed_amount) OVER (PARTITION BY claim_type, policy_id ORDER BY claim_date ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS claimed_amount_sum_3_prior,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY status, adjuster_team), 0), 4) AS claimed_amount_share_of_status_adjuster_team,
        ROW_NUMBER() OVER w_adjuster_id_policy_id_by_cla_2 AS claim_date_seq
    FROM joined_adjuster
    WINDOW w_status_adjuster_team AS (PARTITION BY status, adjuster_team), w_adjuster_id_policy_id_by_cla_2 AS (PARTITION BY adjuster_id, policy_id ORDER BY claim_date, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE claim_date_seq = 1
),
rollup_base AS (
    -- Group-level totals
    SELECT
        adjuster_id,
        SUM(status_adjuster_team_sum_approved_amount) AS total_status_adjuster_team_sum_approved_amount,
        MAX(distinct_adjuster_id_in_status_adjuster_team) AS max_distinct_adjuster_id_in_status_adjuster_team,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        ROUND(SUM(last_known_approved_amount) / NULLIF(SUM(status_adjuster_team_sum_approved_amount), 0), 3) AS last_known_approved_amount_to_status_adjuster_te
    FROM top_rows
    GROUP BY adjuster_id
    HAVING AVG(claim_date_cume_dist) > 0.3
)
-- Final projection
SELECT
    adjuster_id,
    max_distinct_adjuster_id_in_status_adjuster_team,
    n_rows,
    distinct_adjuster_ids,
    last_known_approved_amount_to_status_adjuster_te,
    CASE WHEN n_rows >= 10 THEN 'A' WHEN n_rows >= 5 THEN 'B' ELSE 'C' END AS n_rows_tier,
    FLOOR(distinct_adjuster_ids / 2) * 2 AS distinct_adjuster_ids_bucket
FROM rollup_base
WHERE last_known_approved_amount_to_status_adjuster_te BETWEEN 1.0 AND 9.0
ORDER BY distinct_adjuster_ids_bucket DESC, distinct_adjuster_ids
