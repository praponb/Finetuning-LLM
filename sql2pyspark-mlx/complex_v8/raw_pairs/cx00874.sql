WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        DATE_TRUNC('HOUR', reported_at) AS reported_at_hour,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE claim_type = 'flood'
),
with_details AS (
    -- Join adjusters
    SELECT
        s.*,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        p.end_date AS policy_end_date,
        p.status AS policy_status
    FROM scoped s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id, adjuster_team)) AS distinct_policy_id_in_adjuster_id_adjuster_team,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY policy_status), 0), 4) AS adjuster_level_share_of_policy_status,
        FIRST_VALUE(reported_at_hour) OVER (PARTITION BY claim_type ORDER BY claimed_amount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at_hour,
        SIZE(COLLECT_SET(policy_status) OVER w_adjuster_id_adjuster_team) AS distinct_policy_status_in_adjuster_id_adjuster_t,
        LAST_VALUE(reported_at_hour) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY adjuster_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at_hour,
        ROUND(CUME_DIST() OVER w_policy_id_by_adjuster_level_2, 3) AS adjuster_level_cume_dist,
        ROUND(AVG(claimed_amount_filled) OVER (PARTITION BY MONTH(claim_date) ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_filled_avg_4,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY MONTH(claim_date) ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_2,
        ROUND((UNIX_TIMESTAMP(reported_at_hour) - UNIX_TIMESTAMP(LAG(reported_at_hour) OVER (ORDER BY claimed_amount ASC, id))) / 60, 1) AS minutes_since_prev_reported_at_hour,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(claim_date) ORDER BY claimed_amount NULLS LAST), 3) AS claimed_amount_cume_dist
    FROM with_details
    WINDOW w_adjuster_id_adjuster_team AS (PARTITION BY adjuster_id, adjuster_team), w_policy_id_by_adjuster_level_2 AS (PARTITION BY policy_id ORDER BY adjuster_level)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at_hour,
        claimed_amount_filled,
        adjuster_level,
        distinct_policy_status_in_adjuster_id_adjuster_t,
        last_known_reported_at_hour,
        adjuster_level_cume_dist,
        claimed_amount_filled_avg_4,
        claimed_amount_avg_2,
        minutes_since_prev_reported_at_hour,
        claimed_amount_cume_dist,
        claimed_amount_cume_dist + adjuster_level AS claimed_amount_cume_dist_plus_adjuster_level,
        QUARTER(claim_date) AS claim_date_quarter,
        DATEDIFF(policy_end_date, claim_date) AS days_claim_date_to_policy_end_date
    FROM with_trends
    WHERE adjuster_level_cume_dist >= 0.4
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        adjuster_level,
        SUM(claim_date_quarter) AS total_claim_date_quarter,
        COUNT(*) AS n_rows
    FROM enriched
    WHERE reported_at_hour >= TIMESTAMP '2025-01-21 09:00:00'
    GROUP BY adjuster_level
)
SELECT
    adjuster_level,
    n_rows,
    ABS(total_claim_date_quarter - 16) AS total_claim_date_quarter_distance,
    ROUND(total_claim_date_quarter / NULLIF(adjuster_level, 0), 3) AS total_claim_date_quarter_per_adjuster_level
FROM per_group
ORDER BY n_rows, total_claim_date_quarter_per_adjuster_level
