WITH base AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(supplier_name) AS supplier_name_lower
    FROM suppliers
    WHERE country IN ('TH', 'DE', 'SG')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT id, supplier_name
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC NULLS LAST, id) = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(supplier_name) AS supplier_name_lower
    FROM latest
    WHERE supplier_name LIKE '%Glob%'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IS NOT NULL
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.price >= 420
        )
),
report AS (
    SELECT
        id,
        supplier_name,
        supplier_name_lower
    FROM kept
    ORDER BY id
)
SELECT id
FROM report
ORDER BY id
