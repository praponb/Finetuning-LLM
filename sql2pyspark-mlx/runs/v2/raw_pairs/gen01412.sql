WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) AS row_num
    FROM shipments
)
SELECT *
FROM numbered
WHERE row_num = 1
