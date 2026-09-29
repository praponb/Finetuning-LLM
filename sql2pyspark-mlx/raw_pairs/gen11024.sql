WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age, id) AS rn
    FROM customers
    WHERE name LIKE '%Chaiy%'
)
SELECT *
FROM Ranked
WHERE rn <= 3
