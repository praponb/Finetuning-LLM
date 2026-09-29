WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_label
    FROM claims
    WHERE claim_type = 'flood'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(approved_amount) OVER (ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        ROUND(AVG(approved_amount) OVER (ORDER BY claim_date, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_2,
        LEAD(reported_at) OVER (ORDER BY claim_date, id) AS next_reported_at,
        DENSE_RANK() OVER (ORDER BY claim_date DESC) AS claim_date_dense_rank
    FROM cleaned
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_date >= DATE '2024-07-18'
              AND c.reported_at >= TIMESTAMP '2025-01-31 13:00:00'
        )
),
joined_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.*,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM filtered s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        reported_at_label,
        first_approved_amount,
        approved_amount_avg_2,
        next_reported_at,
        claim_date_dense_rank,
        adjuster_level,
        adjuster_team,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (ORDER BY first_approved_amount DESC, id))) / 60, 1) AS minutes_since_prev_reported_at,
        LEAD(first_approved_amount, 2) OVER (ORDER BY first_approved_amount DESC, id) AS next2_first_approved_amount,
        FIRST_VALUE(next_reported_at) OVER (PARTITION BY status, adjuster_id ORDER BY next_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next_reported_at,
        MAX(approved_amount) OVER (PARTITION BY status, adjuster_id ORDER BY next_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_so_far,
        SIZE(COLLECT_SET(claim_type) OVER w_claim_date_month_adjuster_le) AS distinct_claim_type_in_claim_date_month_adjuster
    FROM joined_adjuster
    WINDOW w_claim_date_month_adjuster_le AS (PARTITION BY MONTH(claim_date), adjuster_level)
)
-- Subtotals with GROUPING SETS
SELECT
    claim_date_dense_rank,
    COALESCE(status, 'ALL') AS status,
    distinct_claim_type_in_claim_date_month_adjuster,
    COUNT(DISTINCT policy_id) AS distinct_policy_ids,
    COUNT(*) AS n_rows
FROM with_trends
GROUP BY GROUPING SETS ((claim_date_dense_rank, status, distinct_claim_type_in_claim_date_month_adjuster), (claim_date_dense_rank), ())
