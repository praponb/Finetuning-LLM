WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SORT_ARRAY(tags) AS sorted_tags,
        COALESCE(brand, 'N/A') AS brand_filled
    FROM products
    WHERE supplier_id IS NOT NULL
)
-- Attach suppliers attributes
SELECT
    src.id,
    src.product_name,
    src.brand,
    src.cost,
    src.tags,
    src.supplier_id,
    src.sorted_tags,
    s.supplier_name,
    s.city
FROM filtered src
LEFT JOIN suppliers s
    ON src.supplier_id = s.id
