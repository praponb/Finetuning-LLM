WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS pos
    FROM order_items
)
SELECT
    id,
    product_id,
    unit_price,
    quantity
FROM numbered
WHERE pos = 1
ORDER BY id DESC;
