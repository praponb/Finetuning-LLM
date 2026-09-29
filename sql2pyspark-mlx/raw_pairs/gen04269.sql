SELECT *
FROM (
    SELECT
        id,
        order_id,
        unit_price,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price, id) AS row_num
    FROM order_items
) AS r
WHERE row_num = 1
ORDER BY unit_price, order_id
