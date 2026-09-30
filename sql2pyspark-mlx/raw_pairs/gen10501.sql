WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        ROUND(quantity * 0.85, 2) AS quantity_adjusted
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-04-05')
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.*,
        s.sqft,
        s.store_name
    FROM filtered src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_id,
        quantity_adjusted,
        sqft
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity_adjusted DESC NULLS LAST, id ASC) AS dedup_rank
        FROM with_dims
    ) t
    WHERE dedup_rank = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM latest
    EXCEPT
    SELECT id, product_id
    FROM inventory
    WHERE last_restock >= '2023-12-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.id = k.id AND s.product_id = k.product_id
)
-- Join a dimension and aggregate
SELECT
    d.category,
    COUNT(*) AS cnt,
    AVG(src.sqft) AS avg_sqft,
    AVG(src.quantity_adjusted) AS avg_quantity_adjusted
FROM reconciled src
LEFT JOIN products d ON src.product_id = d.id
GROUP BY d.category
ORDER BY category DESC NULLS LAST
