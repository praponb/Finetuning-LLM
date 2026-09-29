WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        LENGTH(product_name) AS product_name_length,
        price - cost AS margin,
        COALESCE(brand, 'NONE') AS brand_clean
    FROM products
    WHERE category = 'Electronics'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN margin >= 71 THEN 'LARGE' ELSE 'SMALL' END AS margin_tier,
    category,
    SIZE(COLLECT_SET(category)) AS num_categories,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    APPROX_PERCENTILE(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles
FROM valid
GROUP BY GROUPING SETS ((CASE WHEN margin >= 71 THEN 'LARGE' ELSE 'SMALL' END, category), (CASE WHEN margin >= 71 THEN 'LARGE' ELSE 'SMALL' END), ())
HAVING SIZE(COLLECT_SET(brand)) > 1
