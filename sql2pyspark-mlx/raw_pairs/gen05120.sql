WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        ARRAY_JOIN(tags, ', ') AS tags_text,
        SIZE(tags) AS tags_count
    FROM products
    WHERE supplier_id IS NOT NULL
)
SELECT brand
FROM filtered
EXCEPT DISTINCT
SELECT brand
FROM products
WHERE cost BETWEEN 280 AND 580
