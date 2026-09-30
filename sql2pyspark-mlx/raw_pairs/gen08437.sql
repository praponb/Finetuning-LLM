WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        CASE WHEN rating >= 4.6 THEN 'High' WHEN rating >= 1.6 THEN 'Medium' ELSE 'Low' END AS rating_level,
        REPLACE(supplier_name, ' ', '-') AS supplier_name_slug
    FROM suppliers
    WHERE city IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.brand IN ('Initech', 'Globex')
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        rating_level,
        supplier_name_slug,
        LOWER(city) AS city_lower,
        COALESCE(city, 'NONE') AS city_clean
    FROM eligible
    WHERE city NOT IN ('Tokyo', 'Berlin')
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.product_cost,
        top1.price
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.cost AS product_cost, c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    supplier_name,
    city_lower,
    city_clean,
    price
FROM with_last
ORDER BY id DESC;
