WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        TRUNC(start_date, 'MM') AS start_date_month_start
    FROM policies
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.end_date < DATE '2026-12-05'
              AND p.product = 'HEALTH'
        )
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        start_date_month_start,
        SUM(premium) OVER (PARTITION BY MONTH(start_date_month_start)) AS start_date_month_start_month_sum_premium,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY MONTH(start_date_month_start))) AS distinct_holder_id_in_start_date_month_start_mon,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY start_date_month_start, id), 3) AS start_date_month_start_cume_dist,
        premium - LAG(premium) OVER (PARTITION BY holder_id ORDER BY premium, id) AS premium_change,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date_month_start) ORDER BY premium DESC, id) AS premium_decile
    FROM screened
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(claim_date) AS first_claim_date,
        MAX(claim_date) AS last_claim_date
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_date,
        c.last_claim_date
    FROM ranked s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
SELECT
    distinct_holder_id_in_start_date_month_start_mon,
    premium_decile,
    GROUPING_ID() AS grouping_level,
    DATEDIFF(MAX(start_date), MIN(start_date)) AS start_date_span_days,
    SUM(start_date_month_start_cume_dist) AS total_start_date_month_start_cume_dist,
    PERCENTILE_APPROX(distinct_holder_id_in_start_date_month_start_mon, 0.5) AS median_distinct_holder_id_in_start_date_month_st
FROM profiled
GROUP BY GROUPING SETS ((distinct_holder_id_in_start_date_month_start_mon, premium_decile), (distinct_holder_id_in_start_date_month_start_mon))
