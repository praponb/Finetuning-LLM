WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        ROUND(quantity, 0) AS quantity_rounded,
        CAST(quantity AS DOUBLE) AS quantity_str
    FROM inventory
    WHERE product_id IN (1, 3, 4, 10)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        quantity
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC, id) = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(quantity) AS quantity_whole
    FROM deduped
    WHERE quantity >= 134
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        quantity - quantity_whole AS quantity_minus_quantity_whole,
        COALESCE(quantity_whole, 0) AS quantity_whole_filled
    FROM valid
    WHERE quantity_whole = 41
)
-- Join a dimension and aggregate
SELECT
    dim.region,
    COUNT(*) AS rows_total,
    MIN(e.quantity_whole_filled) AS min_quantity_whole_filled,
    GROUPING_ID() AS level_id
FROM filtered e
LEFT JOIN stores dim ON e.store_id = dim.id
GROUP BY ROLLUP (dim.region)
