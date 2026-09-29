SELECT *
FROM (
    SELECT
        id,
        product_id,
        unit_price,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY unit_price) AS rn
    FROM order_items
    WHERE product_id = 36
) AS t
WHERE rn <= 1
ORDER BY unit_price;
