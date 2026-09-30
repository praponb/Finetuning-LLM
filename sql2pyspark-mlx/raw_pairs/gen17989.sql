SELECT *
FROM (
    SELECT
        order_id,
        product_id,
        COUNT(quantity) quantity_count,
        MAX(unit_price) max_unit_price
    FROM order_items
    GROUP BY order_id, product_id
) sub
WHERE max_unit_price < 320
ORDER BY max_unit_price
