WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ADD_MONTHS(launch_date, 3) AS launch_date_plus_months,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM products
    WHERE launch_date > DATE '2023-05-16'
      AND supplier_id IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date DESC NULLS LAST, id) AS dedup_rank
        FROM base
    ) ranked
    WHERE dedup_rank = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        price,
        stock_qty,
        launch_date,
        tags,
        dedup_rank,
        CAST(cost AS INT) AS cost_int,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM deduped
    WHERE supplier_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        price,
        stock_qty,
        launch_date,
        tags,
        dedup_rank,
        cost_int,
        brand_filled,
        NTILE(3) OVER (PARTITION BY category ORDER BY cost_int, id) AS cost_int_bucket,
        DENSE_RANK() OVER w_grp AS stock_qty_dense_rank,
        CUME_DIST() OVER (PARTITION BY category ORDER BY stock_qty DESC) AS stock_qty_cume_dist
    FROM filtered
    WINDOW w_grp AS (PARTITION BY category ORDER BY stock_qty NULLS FIRST)
)
-- Grouped metrics
SELECT
    category,
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    SIZE(COLLECT_SET(category)) AS num_categories,
    PERCENTILE_APPROX(stock_qty, 0.9) AS stock_qty_p90,
    COUNT(*) AS event_count
FROM metrics
GROUP BY GROUPING SETS ((category, DATE_TRUNC('YEAR', launch_date)), (category), ());
