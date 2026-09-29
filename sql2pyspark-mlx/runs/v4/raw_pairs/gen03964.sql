WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.product_id IN (18, 23, 24, 28)
        )
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY supplier_id ORDER BY stock_qty) AS rn
        FROM kept
    ) x
    WHERE rn = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT category, brand
    FROM deduped
    INTERSECT
    SELECT category, brand
    FROM products
    WHERE cost <= 320
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN remaining_keys k ON s.category = k.category AND s.brand = k.brand
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', brand, category) AS brand_category_key,
        ROUND(stock_qty * 1.1, 2) AS adjusted_stock_qty,
        COALESCE(brand, 'OTHER') AS brand_clean
    FROM kept_rows
    WHERE ARRAY_CONTAINS(tags, 'eco')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock <= s.launch_date
        )
),
cleaned_products AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        price - stock_qty AS price_minus_stock_qty,
        COALESCE(brand, 'Unknown') AS brand_filled
    FROM eligible
    WHERE brand IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(stock_qty, price) OVER (PARTITION BY brand) AS corr_stock_qty_price,
        STDDEV_SAMP(price_minus_stock_qty) OVER w_hist AS supplier_id_stddev_samp_price_minus_stock_qty,
        CUME_DIST() OVER (PARTITION BY category, brand ORDER BY adjusted_stock_qty DESC NULLS FIRST) AS adjusted_stock_qty_cume_dist,
        PERCENT_RANK() OVER (ORDER BY cost DESC) AS cost_percent_rank,
        PERCENT_RANK() OVER w_part AS stock_qty_percent_rank
    FROM cleaned_products
    WINDOW w_hist AS (PARTITION BY supplier_id), w_part AS (PARTITION BY supplier_id, brand ORDER BY stock_qty DESC NULLS LAST)
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN corr_stock_qty_price >= 0.49 THEN 'PREMIUM' ELSE 'STANDARD' END AS corr_stock_qty_price_tier,
        CASE WHEN cost >= 730 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
        BIT_OR(stock_qty) AS stock_qty_bit_or,
        MAX(corr_stock_qty_price) AS max_corr_stock_qty_price,
        COUNT(*) AS num_rows
    FROM metrics
    GROUP BY CASE WHEN corr_stock_qty_price >= 0.49 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN cost >= 730 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    corr_stock_qty_price_tier,
    cost_tier,
    max_corr_stock_qty_price,
    num_rows
FROM grouped;
