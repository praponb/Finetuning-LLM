SELECT *
FROM (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS rnk
    FROM order_items
) AS ranked
WHERE rnk <= 5
ORDER BY quantity DESC;
