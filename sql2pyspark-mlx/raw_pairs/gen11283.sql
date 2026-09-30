WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        ROUND(quantity, 0) AS quantity_rounded,
        DATEDIFF(CURRENT_DATE(), last_restock) / 30 AS months_since_last_restock
    FROM inventory
    WHERE product_id IN (3, 8, 9)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        product_id
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY months_since_last_restock DESC NULLS FIRST, id) = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT e.*, b.region
    FROM latest e
    JOIN stores b
        ON e.store_id = b.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN region = 'Central' THEN 1 ELSE 0 END AS is_central,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM with_dims
    WHERE region IS DISTINCT FROM 'East'
      AND store_id IN (2, 3, 5)
)
SELECT
    id,
    product_id,
    region_clean
FROM valid
ORDER BY product_id DESC NULLS FIRST
