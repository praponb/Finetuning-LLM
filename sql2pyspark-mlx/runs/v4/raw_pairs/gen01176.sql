WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        CAST(revenue AS INT) AS revenue_int,
        LEAST(revenue, units) AS least_revenue_units
    FROM store_sales
    WHERE sale_date > DATE '2023-11-11'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS dedup_rank
        FROM base
    ) x
    WHERE dedup_rank = 1
),
enriched AS (
    -- Attach products attributes
    SELECT a.*, b.stock_qty
    FROM deduped a
    INNER JOIN products b
        ON a.product_id = b.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        revenue_int,
        dedup_rank,
        revenue * stock_qty AS revenue_times_stock_qty,
        least_revenue_units - dedup_rank AS least_revenue_units_minus_dedup_rank
    FROM enriched
    WHERE sale_date > DATE '2023-11-30'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN revenue_int >= 69 THEN 'HIGH' ELSE 'LOW' END AS revenue_int_tier,
    STDDEV_SAMP(revenue) AS revenue_stddev,
    PERCENTILE_APPROX(revenue, 0.95) AS revenue_p95
FROM filtered
GROUP BY CASE WHEN revenue_int >= 69 THEN 'HIGH' ELSE 'LOW' END
ORDER BY revenue_p95 DESC NULLS FIRST
