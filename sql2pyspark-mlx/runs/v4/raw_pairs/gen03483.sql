WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        revenue + units AS revenue_per_unit,
        ROUND(units, 1) AS units_rounded
    FROM store_sales
    WHERE sale_date >= TO_DATE('2023-12-19')
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS row_num
        FROM base
    ) t
    WHERE row_num = 1
),
ranked_store_sales AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue_per_unit DESC, id) AS rn
    FROM ranked
    QUALIFY rn = 1
),
enriched AS (
    -- Attach products attributes
    SELECT e.*, d.stock_qty
    FROM ranked_store_sales e
    INNER JOIN products d
        ON e.product_id = d.id
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN stock_qty >= 314 THEN 'PREMIUM' ELSE 'STANDARD' END AS stock_qty_tier,
        DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
        store_id,
        APPROX_PERCENTILE(revenue, 0.5) AS revenue_p50,
        BIT_AND(units) AS units_bit_and,
        PERCENTILE_APPROX(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(units_rounded) AS total_units_rounded
    FROM enriched
    GROUP BY CASE WHEN stock_qty >= 314 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', sale_date), store_id
)
SELECT
    stock_qty_tier,
    quarter_start,
    store_id,
    units_bit_and,
    units_quartiles,
    total_units_rounded
FROM summary
ORDER BY store_id DESC
