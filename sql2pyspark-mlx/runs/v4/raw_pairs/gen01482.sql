WITH valid AS (
    -- Basic cleansing and defaults
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
        supplier_id,
        INITCAP(TRIM(category)) category_title,
        INITCAP(TRIM(brand)) brand_title,
        COALESCE(brand, 'N/A') brand_filled
    FROM products
    WHERE brand IS NOT NULL
      AND product_name IS NOT NULL
),
valid_products AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ARRAY_JOIN(tags, '|') tags_text,
        COALESCE(brand, 'OTHER') resolved_brand
    FROM valid
    WHERE brand IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(cost / SUM(cost) OVER (PARTITION BY supplier_id), 4) cost_share,
        NTILE(5) OVER (PARTITION BY brand ORDER BY cost DESC, id) cost_bucket
    FROM valid_products
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC, id) AS rn
        FROM enriched
    ) t
    WHERE rn = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(cost, TRUE) OVER (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_cost,
        DENSE_RANK() OVER (PARTITION BY category, supplier_id ORDER BY stock_qty) stock_qty_dense_rank
    FROM latest
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.unit_price > 390
        )
)
SELECT
    id,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    category_title,
    CASE WHEN category RLIKE '^(Electronics|Books)$' THEN 'GROUP_A' WHEN category RLIKE '^(Home|Sports)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS category_zone
FROM not_flagged
ORDER BY price DESC NULLS FIRST, id DESC
