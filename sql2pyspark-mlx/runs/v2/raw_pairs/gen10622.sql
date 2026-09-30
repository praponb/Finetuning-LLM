WITH top_shipments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) AS rnk
    FROM shipments
)
SELECT *
FROM top_shipments
WHERE rnk = 1
