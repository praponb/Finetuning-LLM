WITH base AS (
    SELECT
        *,
        LENGTH(city) city_length,
        CONCAT(country, ' ', supplier_name) country_supplier_name
    FROM suppliers
    WHERE supplier_name LIKE 'Eur%'
)
SELECT
    id,
    supplier_name,
    country,
    rating,
    city_length,
    country_supplier_name
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC NULLS LAST, id) AS dedup_rank
    FROM base
) t
WHERE dedup_rank = 1
