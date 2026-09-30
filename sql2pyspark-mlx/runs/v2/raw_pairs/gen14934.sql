SELECT
    id,
    unit_price,
    order_id,
    quantity + unit_price AS line_total
FROM order_items;
