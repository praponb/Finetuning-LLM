SELECT
    id,
    unit_price,
    quantity,
    LAST_VALUE(quantity) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_quantity
FROM order_items;
