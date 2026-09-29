WITH cleaned AS (
    -- Keep valid rows and derive helper columns
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
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label
    FROM claims
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claim_date_label,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM cleaned s
    JOIN adjusters a ON a.id = s.adjuster_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(adjuster_level) OVER (ORDER BY reported_at ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_2,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), adjuster_id), 2) AS adjuster_level_vs_reported_at_month_adjuster_id_,
        DENSE_RANK() OVER (PARTITION BY policy_id, adjuster_level ORDER BY adjuster_level DESC, id) AS adjuster_level_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(reported_at), adjuster_id ORDER BY approved_amount, id), 3) AS approved_amount_cume_dist,
        COUNT(*) OVER (ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(claimed_amount) OVER w_reported_at_month_adjuster_i AS reported_at_month_adjuster_id_sum_claimed_amount
    FROM with_details
    WINDOW w_reported_at_month_adjuster_i AS (PARTITION BY DATE_TRUNC('MONTH', reported_at), adjuster_id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_type,
    claimed_amount,
    approved_amount,
    status,
    reported_at,
    claim_date_label,
    adjuster_level,
    adjuster_name,
    adjuster_level_vs_reported_at_month_adjuster_id_,
    adjuster_level_dense_rank,
    approved_amount_cume_dist,
    running_count,
    reported_at_month_adjuster_id_sum_claimed_amount,
    CONCAT_WS('-', claim_type, claim_date_label) AS claim_type_claim_date_label_label,
    CONCAT(status, '#', CAST(adjuster_id AS STRING)) AS status_ref
FROM metrics
