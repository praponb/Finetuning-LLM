WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        LENGTH(category) category_length,
        TRIM(product_name) clean_product_name,
        COALESCE(brand, 'NONE') brand_clean
    FROM products
    WHERE brand IN ('Globex', 'Acme', 'Stark')
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.stock_qty,
        s.tags,
        s.brand_clean,
        x.store_sale_units
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (ORDER BY stock_qty DESC, id) stock_qty_bucket,
        AVG(stock_qty) OVER (ORDER BY store_sale_units, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) stock_qty_avg_5,
        SUM(stock_qty) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_stock_qty
    FROM with_last
    WHERE product_name NOT LIKE 'Co%'
),
scored_products AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(cumulative_stock_qty) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_cumulative_stock_qty,
        SUM(store_sale_units) OVER (ORDER BY stock_qty_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_store_sale_units,
        RANK() OVER (ORDER BY store_sale_units) store_sale_units_rank,
        SUM(store_sale_units) OVER (ORDER BY store_sale_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_store_sale_units
    FROM scored
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        cumulative_stock_qty + stock_qty_bucket cumulative_stock_qty_plus_stock_qty_bucket,
        COALESCE(cumulative_stock_qty, 0) cumulative_stock_qty_filled
    FROM scored_products
    WHERE stock_qty IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM filtered
    EXCEPT DISTINCT
    SELECT id
    FROM products
    WHERE cost < 600
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.id = k.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN cumulative_stock_qty_plus_stock_qty_bucket >= 76 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_stock_qty_plus_stock_qty_bucket_tier,
    CASE WHEN cumulative_stock_qty >= 2083 THEN 'HIGH' ELSE 'LOW' END AS cumulative_stock_qty_tier,
    BIT_AND(stock_qty) AS stock_qty_bit_and,
    PERCENTILE_APPROX(stock_qty_avg_5, 0.95) AS stock_qty_avg_5_p95,
    MIN(cumulative_stock_qty) AS min_cumulative_stock_qty,
    RANK() OVER (PARTITION BY CASE WHEN cumulative_stock_qty_plus_stock_qty_bucket >= 76 THEN 'LARGE' ELSE 'SMALL' END ORDER BY MIN(cumulative_stock_qty) DESC) AS min_cumulative_stock_qty_rank
FROM reconciled
GROUP BY 1, 2
ORDER BY stock_qty_avg_5_p95 NULLS FIRST;
