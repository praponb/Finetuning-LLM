WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS pos
    FROM order_items
)
SELECT *
FROM ranked
WHERE pos <= 2
ORDER BY product_id
