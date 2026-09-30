WITH top_shipments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id) AS rnk
    FROM shipments
    WHERE carrier IN ('Flash', 'UPS')
)
SELECT *
FROM top_shipments
WHERE rnk <= 5;
