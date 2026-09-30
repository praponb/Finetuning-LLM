WITH base AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(city) AS clean_city
    FROM suppliers
    WHERE supplier_name LIKE 'Lio%'
)
-- Deduplicate with ROW_NUMBER
SELECT
    id,
    supplier_name,
    country,
    clean_city
FROM base
QUALIFY RANK() OVER (PARTITION BY city ORDER BY rating NULLS FIRST) = 1;
