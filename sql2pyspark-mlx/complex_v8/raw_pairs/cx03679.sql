WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status
    FROM policies
    WHERE product IN ('AUTO', 'HEALTH')
      AND end_date < DATE '2026-12-05'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY holder_id ORDER BY start_date DESC) AS start_date_dense_rank,
        RANK() OVER w_holder_id_by_start_date_ties AS start_date_rank,
        DATEDIFF(start_date, LAG(start_date) OVER w_holder_id_by_start_date) AS days_since_prev_start_date,
        LEAD(start_date, 3) OVER (PARTITION BY holder_id ORDER BY start_date DESC, id) AS next3_start_date,
        FIRST_VALUE(start_date) OVER (PARTITION BY holder_id ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date
    FROM filtered
    WINDOW w_holder_id_by_start_date_ties AS (PARTITION BY holder_id ORDER BY start_date DESC), w_holder_id_by_start_date AS (PARTITION BY holder_id ORDER BY start_date DESC, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        start_date_dense_rank,
        start_date_rank,
        days_since_prev_start_date,
        next3_start_date,
        first_start_date,
        SUM(start_date_dense_rank) OVER (PARTITION BY status ORDER BY next3_start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_start_date_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, start_date_rank ORDER BY start_date ASC), 3) AS start_date_percent_rank
    FROM ranked
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date_dense_rank,
        s.start_date_rank,
        s.next3_start_date,
        s.first_start_date,
        s.running_start_date_dense_rank,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status
    FROM metrics s
    JOIN claims c ON c.policy_id = s.id AND c.status IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_claim_status AS rows_in_claim_status,
        start_date_rank - LAG(start_date_rank) OVER (PARTITION BY claim_status ORDER BY claim_approved_amount, id, claim_id) AS start_date_rank_change,
        SUM(start_date_rank) OVER (PARTITION BY claim_status) AS claim_status_sum_start_date_rank,
        MAX(start_date_dense_rank) OVER w_claim_status AS claim_status_max_start_date_dense_rank
    FROM exploded_detail_claims
    WINDOW w_claim_status AS (PARTITION BY claim_status)
)
-- Final projection
SELECT DISTINCT start_date_dense_rank, first_start_date
FROM with_trends
ORDER BY first_start_date
