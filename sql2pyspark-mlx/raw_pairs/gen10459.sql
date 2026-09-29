SELECT
    product_id,
    MIN(quantity) AS min_quantity,
    CORR(quantity, unit_price) AS corr_quantity_unit_price
FROM order_items
GROUP BY product_id
ORDER BY corr_quantity_unit_price DESC NULLS FIRST
