WITH ranked AS (
    SELECT
        id,
        city,
        rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rn
    FROM suppliers
    WHERE supplier_name IS NOT NULL
)
SELECT *
FROM ranked
WHERE rn = 1
ORDER BY rn DESC;
