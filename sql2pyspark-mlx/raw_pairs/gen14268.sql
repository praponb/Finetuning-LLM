WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        promo,
        price * qty AS price_times_qty,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        COALESCE(promo, 'OTHER') AS resolved_promo
    FROM lines
    WHERE created_at > DATE '2025-03-01'
      AND customer_id IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(promo) AS promo_upper
    FROM base
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 11:00:00')
      AND promo IS NOT NULL
),
base_carts AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        qty / NULLIF(price_times_qty, 0) AS qty_per_price_times_qty,
        COALESCE(promo, 'UNKNOWN') AS promo_filled
    FROM valid
    WHERE created_at > TIMESTAMP '2025-03-01 22:00:00'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(qty) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS qty_count_2h,
        MIN(qty_per_price_times_qty) OVER w_running AS promo_min_qty_per_price_times_qty,
        STDDEV_SAMP(product_id) OVER (PARTITION BY YEAR(created_at), promo) AS created_at_year_stddev_samp_product_id,
        MIN(qty_per_price_times_qty) OVER w_running AS promo_min_qty_per_price_times_qty_2
    FROM base_carts
    WINDOW w_running AS (PARTITION BY promo)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        created_at,
        coupon_codes,
        product_id,
        promo,
        price_times_qty,
        resolved_promo,
        promo_upper,
        qty_per_price_times_qty,
        created_at_year_stddev_samp_product_id,
        promo_min_qty_per_price_times_qty_2,
        CAST(qty_count_2h AS STRING) AS qty_count_2h_str,
        COALESCE(promo, 'UNKNOWN') AS promo_filled_2
    FROM with_windows
    WHERE created_at >= TIMESTAMP '2025-03-01 18:00:00'
      AND promo IS NOT DISTINCT FROM 'SPRING10'
)
-- Grouped metrics
SELECT
    product_id,
    ROUND(AVG(price_times_qty), 2) AS avg_price_times_qty,
    MIN(created_at) AS started_at,
    MIN(product_id) AS min_product_id,
    SUM(product_id) FILTER (WHERE promo = 'WELCOME5') AS welcome5_total_product_id
FROM filtered
GROUP BY product_id;
