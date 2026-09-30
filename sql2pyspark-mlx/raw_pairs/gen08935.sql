WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        UPPER(city) AS city_upper,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM suppliers
    WHERE city = 'Chiang Mai'
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM filtered
    EXCEPT ALL
    SELECT id
    FROM suppliers
    WHERE rating <= 3.0
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT country, id
    FROM reconciled
    EXCEPT ALL
    SELECT country, id
    FROM suppliers
    WHERE rating > 2.7
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN matched_keys k ON s.country = k.country AND s.id = k.id
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category IN ('Electronics', 'Books', 'Fashion')
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        city_upper,
        resolved_city,
        CONCAT_WS('/', country, supplier_name) AS country_supplier_name_key
    FROM eligible
    WHERE city IS NOT NULL
)
SELECT
    city,
    resolved_city,
    country_supplier_name_key
FROM cleaned
