WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        SUBSTRING(city, 1, 3) AS city_prefix,
        INITCAP(TRIM(city)) AS city_title,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM suppliers
    WHERE city <> 'Berlin'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category IN ('Books', 'Fashion')
        )
),
valid_suppliers AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(city) AS city_lower
    FROM eligible
    WHERE city IS NOT NULL
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.city_title,
        s.city_lower,
        latest.product_stock_qty
    FROM valid_suppliers s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND LOWER(TRIM(c.product_name)) = 'water bottle'
        )
)
SELECT id, city_title
FROM not_flagged
ORDER BY id DESC
LIMIT 10;
