WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS pos
    FROM suppliers
)
SELECT *
FROM Numbered
WHERE pos <= 5
ORDER BY id DESC
