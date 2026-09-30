WITH ranked AS (
    SELECT
        id,
        country,
        rating,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rnk
    FROM suppliers
)
SELECT *
FROM ranked
WHERE rnk <= 2
ORDER BY id ASC, 4 ASC;
