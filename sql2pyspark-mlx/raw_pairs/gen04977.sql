WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(TRIM(supplier_name)) supplier_name_normalized
    FROM suppliers
    WHERE city IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.price > 680
        )
),
kept_suppliers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ARRAY_CONTAINS(ch.tags, 'clearance')
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        country,
        rating
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rank_in_group
        FROM kept_suppliers
    ) t
    WHERE rank_in_group = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT country, id
    FROM latest
    INTERSECT ALL
    SELECT country, id
    FROM suppliers
    WHERE rating >= 1.9
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.country = k.country AND s.id = k.id
),
kept_suppliers_2 AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id = 25
        )
)
SELECT
    id,
    country,
    rating
FROM kept_suppliers_2;
