WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE claimed_amount < 31752.9
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.reported_at,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM prepared_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE LENGTH(TRIM(a.adjuster_name)) >= 7
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        adjuster_level,
        adjuster_team,
        adjuster_name,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY adjuster_id), 2) AS approved_amount_vs_adjuster_id_avg,
        ROUND(CUME_DIST() OVER w_claim_date_month_adjuster_id, 3) AS claim_date_cume_dist,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        COUNT(*) OVER w_adjuster_id AS rows_in_adjuster_id,
        LAG(approved_amount, 3) OVER w_claim_date_month_adjuster_id_3 AS prev3_approved_amount,
        MIN(approved_amount) OVER w_adjuster_id AS adjuster_id_min_approved_amount,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY adjuster_team, adjuster_level ORDER BY adjuster_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER w_adjuster_id, 0), 4) AS adjuster_level_share_of_adjuster_id,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY adjuster_team, adjuster_level ORDER BY adjuster_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY adjuster_team, adjuster_level ORDER BY adjuster_level, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_7,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_approved_amount
    FROM joined
    WINDOW w_claim_date_month_adjuster_id AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id ORDER BY claim_date), w_adjuster_id AS (PARTITION BY adjuster_id), w_claim_date_month_adjuster_id_3 AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id ORDER BY claim_date, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    adjuster_level,
    adjuster_team,
    adjuster_name,
    claim_date_cume_dist,
    running_count,
    rows_in_adjuster_id,
    prev3_approved_amount,
    adjuster_id_min_approved_amount,
    first_known_reported_at,
    adjuster_level_share_of_adjuster_id,
    second_approved_amount,
    adjuster_id_avg_approved_amount,
    ABS(claim_date_cume_dist - 0.7) AS claim_date_cume_dist_distance
FROM with_windows
ORDER BY id, second_approved_amount DESC
