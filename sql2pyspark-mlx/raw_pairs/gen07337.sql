WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS pos
    FROM suppliers
    WHERE country NOT IN ('TH', 'US', 'SG')
)
SELECT
    id,
    country,
    rating,
    city
FROM numbered
WHERE pos <= 2
