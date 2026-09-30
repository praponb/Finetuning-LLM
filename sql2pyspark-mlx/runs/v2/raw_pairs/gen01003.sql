SELECT
    id,
    product_id,
    unit_price - quantity AS line_total,
    COALESCE(unit_price, 0) AS unit_price_filled
FROM order_items
