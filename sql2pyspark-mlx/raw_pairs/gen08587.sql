WITH ranked_shipments AS (
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY cost) AS rnk
    FROM shipments
)
SELECT *
FROM ranked_shipments
WHERE rnk = 1
ORDER BY rnk DESC
