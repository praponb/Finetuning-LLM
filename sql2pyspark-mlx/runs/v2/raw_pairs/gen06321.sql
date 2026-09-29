SELECT
    id,
    quantity,
    order_id,
    CEIL(quantity) AS quantity_whole,
    COALESCE(unit_price, 0) AS unit_price_filled
FROM order_items
