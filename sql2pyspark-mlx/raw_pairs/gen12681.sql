WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating NULLS LAST, id) AS rn
    FROM suppliers
    QUALIFY rn = 1
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    rn
FROM deduped
ORDER BY rn DESC, country;
