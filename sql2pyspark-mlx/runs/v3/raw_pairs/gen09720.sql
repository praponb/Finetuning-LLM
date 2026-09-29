SELECT *
FROM (
    SELECT
        id,
        product_id,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS rn
    FROM order_items
) AS r
WHERE rn <= 5
ORDER BY rn DESC;
