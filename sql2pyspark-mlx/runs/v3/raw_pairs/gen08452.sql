WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) AS rnk
    FROM shipments
)
SELECT
    id,
    carrier,
    shipped_date,
    delivered_date
FROM Ranked
WHERE rnk <= 5;
