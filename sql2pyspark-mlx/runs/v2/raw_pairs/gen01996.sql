SELECT
    id,
    order_id,
    unit_price,
    ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id), 3) AS unit_price_share,
    ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id), 3) AS unit_price_share_2
FROM order_items
