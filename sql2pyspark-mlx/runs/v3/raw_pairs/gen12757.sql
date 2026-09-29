SELECT
    id,
    order_id,
    product_id,
    quantity,
    CEIL(unit_price) AS unit_price_whole
FROM order_items
ORDER BY unit_price_whole ASC;
