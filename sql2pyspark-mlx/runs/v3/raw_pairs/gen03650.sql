SELECT
    id,
    order_id,
    product_id,
    CEIL(unit_price) AS unit_price_whole,
    ROUND(unit_price * 1.1, 2) AS adjusted_unit_price
FROM order_items
WHERE unit_price IS NOT NULL OR product_id IS NOT NULL
ORDER BY unit_price_whole DESC;
