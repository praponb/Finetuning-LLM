WITH cleaned AS (
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM order_items
    WHERE quantity = 5
)
SELECT
    id,
    order_id,
    SUM(quantity) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
    LAST_VALUE(quantity_filled, TRUE) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_filled,
    SUM(unit_price) OVER (PARTITION BY product_id) AS product_id_sum_unit_price
FROM cleaned
WHERE order_id IN (10, 13, 32, 33)
