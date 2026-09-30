WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        CASE WHEN stock_qty >= 392 THEN 'High' WHEN stock_qty >= 143 THEN 'Medium' ELSE 'Low' END AS stock_qty_band
    FROM products
    WHERE launch_date >= DATE '2023-06-07'
      AND stock_qty = 132
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, supplier_id ORDER BY price DESC, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY stock_qty DESC, id) AS row_seq,
        SUM(cost) OVER (PARTITION BY supplier_id, brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        SUM(stock_qty) OVER (PARTITION BY brand) AS brand_sum_stock_qty
    FROM deduped
),
enriched_products AS (
    -- Attach suppliers attributes
    SELECT
        e.id,
        e.category,
        e.cost,
        e.supplier_id,
        e.cumulative_cost,
        p.country,
        p.city
    FROM enriched e
    INNER JOIN suppliers p
        ON e.supplier_id = p.id
    WHERE p.rating BETWEEN 2.2 AND 2.3
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cost >= 730 THEN 'HIGH' ELSE 'LOW' END AS cost_tier,
    country,
    COUNT(DISTINCT country) AS distinct_countries,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    COUNT(DISTINCT country) AS distinct_countries_2
FROM enriched_products
GROUP BY CASE WHEN cost >= 730 THEN 'HIGH' ELSE 'LOW' END, country
HAVING COUNT(DISTINCT country) > 1;
