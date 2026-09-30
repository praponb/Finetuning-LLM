WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rnk
    FROM order_items
)
SELECT
    id,
    product_id,
    quantity,
    order_id
FROM numbered
WHERE rnk <= 3;
