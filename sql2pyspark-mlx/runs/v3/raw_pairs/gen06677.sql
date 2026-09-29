SELECT
    id,
    product_id,
    unit_price,
    ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 3) AS unit_price_share
FROM order_items
ORDER BY id DESC
