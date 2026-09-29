WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rnk
    FROM order_items
    WHERE order_id IN (16, 20)
)
SELECT *
FROM numbered
WHERE rnk = 1
ORDER BY product_id DESC
