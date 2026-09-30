WITH top_shipments AS (
    SELECT *, RANK() OVER (PARTITION BY carrier ORDER BY delivered_date DESC) AS rnk
    FROM shipments
)
SELECT
    id,
    carrier,
    delivered_date,
    cost
FROM top_shipments
WHERE rnk <= 3;
