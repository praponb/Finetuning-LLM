WITH with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.category,
        p.price,
        p.cost
    FROM inventory a
    LEFT JOIN products p
        ON a.product_id = p.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, price + quantity AS price_plus_quantity
    FROM with_dims
    WHERE category IS DISTINCT FROM 'Electronics'
      AND last_restock > DATE '2023-03-11'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, product_id
    FROM filtered
    INTERSECT
    SELECT id, product_id
    FROM inventory
    WHERE last_restock < '2025-09-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.product_id = k.product_id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.quantity,
        e.last_restock,
        e.category,
        e.cost,
        s.store_name
    FROM verified e
    LEFT JOIN stores s
        ON e.store_id = s.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        quantity,
        store_name,
        SUBSTRING(category, 1, 2) AS category_prefix
    FROM enriched
    WHERE last_restock >= DATE '2023-03-13'
      AND category IS NOT NULL
)
SELECT
    store_name,
    category_prefix,
    CASE WHEN store_name RLIKE '^(Airport Kiosk|Mall Central)$' THEN 'GROUP_A' WHEN store_name RLIKE '^(Siam Flagship|Downtown Express)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS store_name_zone
FROM base;
