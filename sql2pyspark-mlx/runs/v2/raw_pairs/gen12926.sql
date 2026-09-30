WITH numbered AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS rn
    FROM order_items
)
SELECT *
FROM numbered
WHERE rn <= 5;
