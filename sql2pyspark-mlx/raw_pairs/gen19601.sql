WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(supplier_name) AS clean_supplier_name,
        INITCAP(TRIM(country)) AS country_title
    FROM suppliers
    WHERE city IS DISTINCT FROM 'Tokyo'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        supplier_name,
        country,
        city,
        country_title
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS FIRST) = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(country) AS clean_country
    FROM latest
    WHERE city IS NOT NULL
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date > '2025-02-01'
        )
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.country,
        s.city,
        s.country_title,
        s.clean_country,
        latest.brand
    FROM eligible s
    JOIN LATERAL (
            SELECT c.brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    supplier_name,
    country,
    city,
    clean_country,
    brand,
    CASE WHEN country RLIKE '^(DE|SG)$' THEN 'CORE' WHEN country RLIKE '^(TH|US)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS country_zone
FROM with_latest
ORDER BY id DESC NULLS FIRST
LIMIT 10;
