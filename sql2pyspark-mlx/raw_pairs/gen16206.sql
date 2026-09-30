WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CASE WHEN unit_price >= 340 THEN 'High' WHEN unit_price >= 80 THEN 'Medium' ELSE 'Low' END AS unit_price_band,
        unit_price + quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (4, 13, 23)
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.unit_price_band,
        s.line_total,
        d.brand
    FROM valid s
    JOIN products d
        ON s.product_id = d.id
),
enriched_order_items AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.line_total,
        s.brand,
        b.cost,
        b.brand AS product_brand,
        b.stock_qty
    FROM enriched s
    LEFT JOIN products b
        ON s.product_id = b.id
    WHERE YEAR(b.launch_date) >= 2024
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(cost * 1.07, 2) AS cost_adjusted,
        COALESCE(product_brand, 'N/A') AS resolved_product_brand
    FROM enriched_order_items
    WHERE product_brand IS NOT NULL
      AND brand IS DISTINCT FROM 'Globex'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cost DESC, id) AS row_seq,
        LAST_VALUE(cost, TRUE) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost
    FROM base
)
-- Aggregate with conditional metrics
SELECT
    product_brand,
    COUNT(*) FILTER (WHERE product_brand = 'Acme') AS acme_rows,
    APPROX_PERCENTILE(cost_adjusted, ARRAY(0.25, 0.5, 0.75)) AS cost_adjusted_quartiles,
    COUNT(CASE WHEN brand = 'Globex' THEN 1 END) AS globex_count
FROM metrics
GROUP BY product_brand
