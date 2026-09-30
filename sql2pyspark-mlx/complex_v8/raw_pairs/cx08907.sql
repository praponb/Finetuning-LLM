WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        LOWER(TRIM(product)) AS product_clean,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE end_date BETWEEN DATE '2025-04-17' AND DATE '2027-01-03'
      AND holder_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        start_date,
        status,
        premium_filled,
        ROUND(premium_bucket / NULLIF(SUM(premium_bucket) OVER w_start_date_month_product, 0), 4) AS premium_bucket_share_of_start_date_month_product,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY status, holder_id ORDER BY premium_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        MAX(premium_filled) OVER (PARTITION BY MONTH(start_date), product) AS start_date_month_product_max_premium_filled
    FROM base
    WINDOW w_start_date_month_product AS (PARTITION BY MONTH(start_date), product)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        holder_id,
        YEAR(last_known_start_date) AS last_known_start_date_year,
        PERCENTILE_APPROX(start_date_month_product_max_premium_filled, 0.5) AS median_start_date_month_product_max_premium_fill,
        ROUND(SUM(start_date_month_product_max_premium_filled) / NULLIF(SUM(premium_bucket_share_of_start_date_month_product), 0), 3) AS start_date_month_product_max_premium_filled_to_p,
        COUNT(*) AS cnt,
        ROUND(SUM(start_date_month_product_max_premium_filled), 2) AS total_start_date_month_product_max_premium_fille,
        MIN(last_known_start_date) AS first_last_known_start_date
    FROM windowed
    GROUP BY DATE_TRUNC('MONTH', start_date), holder_id, YEAR(last_known_start_date)
    HAVING SUM(premium_filled) > 1640.0
),
with_activity AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM rollup_base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium > 2289.0
              AND p.end_date BETWEEN DATE '2025-04-17' AND DATE '2026-08-31'
        )
)
SELECT
    start_date_month,
    holder_id,
    last_known_start_date_year,
    median_start_date_month_product_max_premium_fill,
    start_date_month_product_max_premium_filled_to_p,
    cnt,
    first_last_known_start_date,
    ABS(cnt - 8) AS cnt_distance
FROM with_activity
ORDER BY start_date_month DESC NULLS FIRST, start_date_month_product_max_premium_filled_to_p
