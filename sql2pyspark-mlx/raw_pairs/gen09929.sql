WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        ROUND(stock_qty, 0) AS stock_qty_rounded,
        CONCAT(category, '/', product_name) AS category_product_name,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM products
    WHERE brand IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        price,
        cost,
        supplier_id,
        stock_qty_rounded,
        MAX(stock_qty_rounded) OVER w_ordered AS running_max_stock_qty_rounded,
        COUNT(stock_qty_rounded) OVER w_ordered AS running_count_stock_qty_rounded,
        MIN(stock_qty_rounded) OVER w_ordered AS running_min_stock_qty_rounded
    FROM filtered
    WHERE stock_qty_rounded >= 72
    WINDOW w_ordered AS (PARTITION BY category, supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, supplier_id
    FROM enriched
    INTERSECT ALL
    SELECT id, supplier_id
    FROM products
    WHERE cost BETWEEN 220 AND 510
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT id
    FROM kept_rows
    EXCEPT ALL
    SELECT id
    FROM products
    WHERE cost > 220
),
kept_rows_products AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    JOIN kept_keys_2 k ON s.id = k.id
),
enriched_products AS (
    -- Attach suppliers attributes
    SELECT
        e.cost,
        e.supplier_id,
        e.running_max_stock_qty_rounded,
        e.running_min_stock_qty_rounded,
        s.city,
        s.rating,
        s.country
    FROM kept_rows_products e
    LEFT JOIN suppliers s
        ON e.supplier_id = s.id
)
-- Grouped metrics
SELECT
    CASE WHEN rating >= 3.5 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    CASE WHEN running_min_stock_qty_rounded >= 75 THEN 'HIGH' ELSE 'LOW' END AS running_min_stock_qty_rounded_tier,
    COUNT_IF(country = 'US') AS us_events,
    PERCENTILE_APPROX(running_min_stock_qty_rounded, ARRAY(0.25, 0.5, 0.75)) AS running_min_stock_qty_rounded_quartiles,
    MAX(running_min_stock_qty_rounded) FILTER (WHERE country = 'TH') AS th_max_running_min_stock_qty_rounded,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count
FROM enriched_products
GROUP BY rating_tier, running_min_stock_qty_rounded_tier
ORDER BY us_events DESC
