WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category <> 'Sports'
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND ARRAY_CONTAINS(x.tags, 'bestseller')
        )
),
kept_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.brand = 'Stark'
        )
),
kept_suppliers_2 AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.stock_qty <> 144
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS row_num
        FROM kept_suppliers_2
    ) ranked
    WHERE row_num = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category IN ('Grocery', 'Sports')
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        row_num,
        CONCAT_WS('/', supplier_name, country) AS supplier_name_country_key,
        LENGTH(supplier_name) AS supplier_name_length
    FROM not_flagged
    WHERE city IS NOT NULL
)
SELECT
    id,
    supplier_name,
    country,
    row_num,
    supplier_name_country_key,
    supplier_name_length,
    CASE WHEN country RLIKE '^(UK|TH)$' THEN 'CORE' WHEN country RLIKE '^(US|SG)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS country_zone
FROM cleaned
ORDER BY id DESC, supplier_name_length
