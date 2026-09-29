WITH filtered AS (
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        LOWER(category) AS category_lower
    FROM products
    WHERE launch_date >= DATE '2023-03-18'
      AND price >= 930
),
cleaned AS (
    SELECT *, ARRAY_JOIN(tags, '|') AS tags_text
    FROM filtered
    WHERE brand IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.unit_price >= 240
        )
),
scored AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY brand ORDER BY price, id) AS price_bucket,
        SUM(stock_qty) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty
    FROM kept
),
base AS (
    SELECT *, CASE WHEN brand IN ('Globex', 'Acme') THEN 'Priority' ELSE 'Other' END AS brand_group
    FROM scored
    WHERE brand IS DISTINCT FROM 'Acme'
      AND cost BETWEEN 230 AND 520
)
SELECT
    id,
    brand,
    price,
    category_lower,
    tags_text,
    price_bucket,
    running_stock_qty,
    brand_group
FROM base
ORDER BY price_bucket DESC NULLS FIRST
