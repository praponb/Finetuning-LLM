SELECT
    id,
    order_id,
    quantity,
    unit_price,
    RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS quantity_rank,
    LAG(quantity, 2, 0) OVER (ORDER BY unit_price, id) AS prev_quantity
FROM order_items;
