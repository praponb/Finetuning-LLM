WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        INITCAP(TRIM(supplier_name)) AS supplier_name_title,
        LOWER(country) AS country_lower,
        COALESCE(city, 'OTHER') AS city_filled
    FROM suppliers
    WHERE supplier_name LIKE 'P%'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.stock_qty <> 126
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        supplier_name_title,
        city_filled,
        CONCAT(supplier_name, '/', country) AS supplier_name_country
    FROM kept
    WHERE supplier_name IS NOT NULL
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.supplier_name_title,
        s.supplier_name_country,
        latest.product_launch_date
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.product_name LIKE 'C%'
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', product_launch_date) AS activity_month,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM kept_suppliers
GROUP BY activity_month
HAVING COUNT(*) >= 1
