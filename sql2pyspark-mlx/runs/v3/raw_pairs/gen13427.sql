WITH Numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY segment ORDER BY age DESC) AS rnk
    FROM customers
)
SELECT *
FROM Numbered
WHERE rnk <= 3
ORDER BY city
