WITH base AS (
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
        DATEDIFF(DATE '2025-06-30', reported_at) AS days_since_reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE approved_amount > 7204.1
),
with_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.days_since_reported_at,
        s.status_or_unknown,
        a.team AS adjuster_team,
        p.start_date AS policy_start_date
    FROM base s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claimed_amount,
        status,
        days_since_reported_at,
        status_or_unknown,
        adjuster_team,
        policy_start_date,
        DENSE_RANK() OVER (PARTITION BY status_or_unknown ORDER BY reported_at) AS reported_at_dense_rank,
        SUM(claimed_amount) OVER (PARTITION BY status_or_unknown ORDER BY reported_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS claimed_amount_sum_3,
        ROUND(AVG(days_since_reported_at) OVER w_status_status_or_unknown, 2) AS status_status_or_unknown_avg_days_since_reported,
        ROUND(CUME_DIST() OVER w_status_or_unknown_by_reporte, 3) AS reported_at_cume_dist,
        COUNT(*) OVER (PARTITION BY status_or_unknown ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(reported_at) OVER (PARTITION BY status_or_unknown ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at
    FROM with_details_adjuster
    WINDOW w_status_status_or_unknown AS (PARTITION BY status, status_or_unknown), w_status_or_unknown_by_reporte AS (PARTITION BY status_or_unknown ORDER BY reported_at)
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.status,
        s.days_since_reported_at,
        s.adjuster_team,
        s.policy_start_date,
        s.reported_at_dense_rank,
        s.status_status_or_unknown_avg_days_since_reported,
        s.running_count,
        s.first_reported_at,
        c.id AS claim_id,
        c.claim_type,
        c.claim_date,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at
    FROM ranked s
    JOIN claims c ON c.policy_id = s.policy_id
)
SELECT
    id,
    adjuster_team,
    policy_start_date,
    status_status_or_unknown_avg_days_since_reported,
    running_count,
    first_reported_at,
    claim_type,
    claim_date,
    claim_approved_amount,
    claim_reported_at
FROM with_children
WHERE claim_date BETWEEN DATE '2024-02-23' AND DATE '2025-09-14'
ORDER BY claim_reported_at, id
