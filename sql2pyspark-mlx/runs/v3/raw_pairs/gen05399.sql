WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) AS rnk
    FROM shipments
)
SELECT *
FROM ranked
WHERE rnk <= 3;
