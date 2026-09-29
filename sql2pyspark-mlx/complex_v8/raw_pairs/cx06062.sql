WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        DATEDIFF(DATE '2026-01-01', end_date) AS days_since_end_date,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE end_date IS NOT NULL
      AND product <> 'TRAVEL'
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        holder_id,
        COUNT(*) AS n_rows,
        MIN(premium) AS min_premium,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        ROUND(AVG(premium), 2) AS avg_premium
    FROM base
    GROUP BY DATE_TRUNC('MONTH', start_date), DATE_TRUNC('MONTH', end_date), holder_id
),
with_parent_holder AS (
    -- Join policyholders
    SELECT
        s.start_date_month,
        s.end_date_month,
        s.holder_id,
        s.n_rows,
        s.min_premium,
        s.avg_premium,
        p.region AS holder_region
    FROM per_group s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
qualified AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_parent_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.product IS NOT NULL
              AND YEAR(p.start_date) = 2024
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        start_date_month,
        end_date_month,
        holder_id,
        min_premium,
        holder_region,
        SUM(avg_premium) OVER (PARTITION BY holder_id) AS holder_id_sum_avg_premium,
        FIRST_VALUE(end_date_month) IGNORE NULLS OVER (PARTITION BY n_rows ORDER BY min_premium DESC, start_date_month, end_date_month, holder_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date_month,
        SUM(n_rows) OVER (PARTITION BY n_rows ORDER BY min_premium DESC, start_date_month, end_date_month, holder_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS n_rows_sum_6_prior
    FROM qualified
)
SELECT
    start_date_month,
    end_date_month,
    holder_id,
    min_premium,
    holder_region,
    holder_id_sum_avg_premium,
    first_known_end_date_month,
    n_rows_sum_6_prior
FROM windowed
WHERE holder_id_sum_avg_premium > 12650.0
ORDER BY end_date_month NULLS LAST
