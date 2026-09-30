WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(cost) AS cost_whole,
        DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date,
        COALESCE(brand, 'N/A') AS brand_filled
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'sale')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC, id) AS dedup_rank
        FROM cleaned
    ) ranked
    WHERE dedup_rank = 1
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.product_name,
        a.category,
        a.price,
        a.stock_qty,
        a.tags,
        a.supplier_id,
        a.cost_whole,
        a.days_since_launch_date,
        a.brand_filled,
        a.dedup_rank,
        p.supplier_name
    FROM deduped a
    LEFT JOIN suppliers p
        ON a.supplier_id = p.id
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.category,
        src.stock_qty,
        src.tags,
        src.supplier_id,
        src.dedup_rank,
        src.supplier_name,
        b.city,
        b.supplier_name AS supplier_supplier_name
    FROM enriched src
    INNER JOIN suppliers b
        ON src.supplier_id <=> b.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.product_id IN (1, 10)
        )
),
with_dims_products AS (
    -- Attach suppliers attributes
    SELECT e.*, p.supplier_name AS supplier_supplier_name_2
    FROM kept e
    LEFT JOIN suppliers p
        ON e.supplier_id <=> p.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_dims_products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty DESC, id) = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        supplier_id,
        category,
        stock_qty
    FROM latest
) src
PIVOT (
    SUM(stock_qty)
    FOR category IN ('Grocery', 'Fashion')
)
