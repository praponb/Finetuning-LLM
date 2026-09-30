WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', supplier_name, country) AS supplier_name_country_key
    FROM suppliers
    WHERE rating > 4.1
),
kept_keys AS (
    -- Set difference on keys
    SELECT country
    FROM filtered
    EXCEPT
    SELECT country
    FROM suppliers
    WHERE UPPER(supplier_name) LIKE '%OKY%'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.country = k.country
),
filtered_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        supplier_name_country_key,
        CONCAT(city, ' - ', country) AS city_country
    FROM verified
    WHERE city IS NOT NULL
      AND supplier_name IS NOT NULL
)
SELECT
    id,
    country,
    city,
    supplier_name_country_key,
    city_country
FROM filtered_suppliers
ORDER BY id DESC NULLS FIRST;
