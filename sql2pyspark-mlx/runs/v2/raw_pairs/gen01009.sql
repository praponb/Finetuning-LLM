WITH ranked_suppliers AS (
    SELECT
        id,
        city,
        rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS pos
    FROM suppliers
    WHERE rating <= 1.8
)
SELECT *
FROM ranked_suppliers
WHERE pos <= 2
ORDER BY rating DESC
