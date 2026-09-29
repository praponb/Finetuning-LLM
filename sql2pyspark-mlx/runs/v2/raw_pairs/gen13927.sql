WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY unit_price) AS pos
    FROM order_items
    WHERE unit_price BETWEEN 220 AND 400
)
SELECT *
FROM numbered
WHERE pos <= 2
