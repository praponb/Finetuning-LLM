WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        price,
        cost,
        tags,
        supplier_id,
        COALESCE(brand, 'N/A') AS brand_filled,
        COALESCE(brand, 'N/A') AS brand_clean
    FROM products
    WHERE supplier_id IS NOT NULL
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.tags,
        s.brand_filled,
        top1.unit_price
    FROM filtered s
    JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.order_id IN (1, 7, 10, 14)
        )
),
filtered_products AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SIZE(tags) AS tags_count
    FROM kept
    WHERE unit_price <= 110
)
SELECT
    id,
    tags,
    brand_filled
FROM filtered_products
ORDER BY id DESC NULLS LAST
