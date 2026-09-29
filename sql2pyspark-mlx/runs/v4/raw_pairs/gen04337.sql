WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        category,
        cost,
        launch_date,
        tags,
        supplier_id,
        stock_qty * cost stock_qty_times_cost,
        COALESCE(brand, 'NONE') resolved_brand
    FROM products
    WHERE brand = 'Acme'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(cost) IGNORE NULLS OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_cost,
        SUM(cost) OVER (PARTITION BY supplier_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_cost
    FROM base
),
matched_keys AS (
    -- Set intersection on keys
    SELECT supplier_id, category
    FROM enriched
    INTERSECT
    SELECT supplier_id, category
    FROM products
    WHERE launch_date > '2023-04-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.supplier_id = k.supplier_id AND s.category = k.category
)
-- Aggregate with conditional metrics
SELECT
    supplier_id,
    APPROX_PERCENTILE(cost, 0.95) AS cost_p95,
    PERCENTILE_APPROX(cumulative_cost, ARRAY(0.25, 0.5, 0.75)) AS cumulative_cost_quartiles,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles
FROM verified
GROUP BY CUBE (supplier_id)
ORDER BY supplier_id NULLS LAST
