WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE status IN ('EXPIRED', 'CANCELLED')
      AND end_date >= DATE '2026-01-24'
),
tagged AS (
    -- Label and score rows
    SELECT *, YEAR(end_date) AS end_date_year
    FROM base
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        end_date_year,
        LAG(premium) OVER (ORDER BY premium, id) AS prev_premium,
        DATEDIFF(end_date, LAG(end_date) OVER w_holder_id_by_premium) AS days_since_prev_end_date,
        MAX(end_date_year) OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_end_date_year_so_far,
        premium_bucket - LAG(premium_bucket) OVER (PARTITION BY holder_id ORDER BY premium DESC, id) AS premium_bucket_change,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY holder_id, status ORDER BY premium_bucket, id)) AS days_since_prev_end_date_2,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY holder_id ORDER BY premium DESC, id)) AS days_since_prev_start_date
    FROM tagged
    WINDOW w_holder_id_by_premium AS (PARTITION BY holder_id ORDER BY premium DESC, id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        end_date,
        end_date_year,
        prev_premium,
        max_end_date_year_so_far,
        premium_bucket_change,
        days_since_prev_end_date_2,
        MAX(premium_bucket_change) OVER (PARTITION BY product, holder_id) AS product_holder_id_max_premium_bucket_change,
        ROUND(AVG(end_date_year) OVER (ORDER BY days_since_prev_end_date_2, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS end_date_year_avg_1_prior,
        ROUND(AVG(days_since_prev_end_date_2) OVER (PARTITION BY MONTH(end_date) ORDER BY days_since_prev_end_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS days_since_prev_end_date_2_avg_1_prior,
        end_date_year - LAG(end_date_year) OVER (ORDER BY days_since_prev_end_date_2, id) AS end_date_year_change,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(end_date), holder_id ORDER BY prev_premium DESC), 3) AS prev_premium_cume_dist
    FROM with_windows
)
-- Subtotals with ROLLUP
SELECT
    holder_id,
    end_date_year,
    GROUPING_ID() AS gid,
    SUM(CASE WHEN product NOT IN ('AUTO', 'HEALTH') THEN 1 ELSE 0 END) AS n_product_not_in_auto_health,
    PERCENTILE_APPROX(days_since_prev_end_date_2, 0.5) AS median_days_since_prev_end_date_2
FROM windowed
GROUP BY ROLLUP(holder_id, end_date_year)
