WITH with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.last_restock,
        x.quantity
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        category,
        brand,
        stock_qty,
        tags,
        supplier_id,
        quantity
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price NULLS LAST, id) = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category, supplier_id ORDER BY quantity DESC, id) AS latest_rank
        FROM latest
    ) t
    WHERE latest_rank = 1
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.tags,
        a.supplier_id,
        a.quantity,
        p.supplier_name,
        p.country,
        p.rating
    FROM ranked a
    LEFT JOIN suppliers p
        ON a.supplier_id = p.id
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        s.rating AS supplier_rating,
        s.supplier_name AS supplier_supplier_name,
        s.country AS supplier_country
    FROM joined src
    JOIN suppliers s
        ON src.supplier_id <=> s.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        supplier_id,
        supplier_country,
        quantity
    FROM enriched
) src
PIVOT (
    MAX(quantity) AS value_sum,
    COUNT(1) AS cnt
    FOR supplier_country IN ('US', 'TH', 'UK')
)
