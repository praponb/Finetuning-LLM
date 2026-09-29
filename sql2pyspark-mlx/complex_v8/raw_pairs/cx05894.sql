WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label
    FROM policies
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.start_date_label,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.reported_at AS claim_reported_at
    FROM base_policys s
    JOIN claims c ON c.policy_id = s.id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        DATE_FORMAT(claim_reported_at, 'yyyy-MM') AS claim_reported_at_ym,
        COUNT(*) FILTER (WHERE status = 'ACTIVE') AS n_status_active,
        COUNT(*) FILTER (WHERE status <> 'LAPSED') AS n_status_lapsed,
        COUNT(DISTINCT claim_id) AS distinct_claim_ids
    FROM with_children
    GROUP BY DATE_TRUNC('MONTH', start_date), DATE_FORMAT(claim_reported_at, 'yyyy-MM')
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(n_status_active / 2) * 2 AS n_status_active_bucket,
        DAYOFWEEK(start_date_month) AS start_date_month_dayofweek,
        ABS(n_status_lapsed - 5) AS n_status_lapsed_distance
    FROM rollup_base
)
-- Final projection
SELECT
    start_date_month,
    claim_reported_at_ym,
    n_status_lapsed,
    n_status_active_bucket,
    start_date_month_dayofweek,
    n_status_lapsed_distance
FROM annotated
ORDER BY n_status_lapsed_distance NULLS LAST, start_date_month DESC
