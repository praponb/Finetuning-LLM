SELECT *
FROM (
    SELECT
        id,
        order_id,
        unit_price,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS pos
    FROM order_items
) AS t
WHERE pos <= 3;
