WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS pos
    FROM order_items
)
SELECT *
FROM ranked
WHERE pos <= 3
ORDER BY 2 DESC, id;
