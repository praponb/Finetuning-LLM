WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE status = 'EXPIRED'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(premium) OVER (ORDER BY start_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_3_prior,
        DATEDIFF(start_date, LAG(start_date) OVER (ORDER BY start_date DESC, id)) AS days_since_prev_start_date,
        ROUND(AVG(premium) OVER (PARTITION BY MONTH(start_date), holder_id), 2) AS start_date_month_holder_id_avg_premium
    FROM valid_policys
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_avg_3_prior,
        s.days_since_prev_start_date,
        s.start_date_month_holder_id_avg_premium,
        p.holder_name
    FROM with_windows s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.holder_name IS NOT NULL
)
-- Subtotals with GROUPING SETS
SELECT
    status,
    holder_id,
    COUNT(*) AS cnt,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list,
    SUM(days_since_prev_start_date) AS total_days_since_prev_start_date
FROM with_parent
GROUP BY GROUPING SETS ((status, holder_id), (status))
