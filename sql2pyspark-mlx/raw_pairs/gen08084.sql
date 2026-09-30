SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    MIN(unit_price) OVER w_ordered last4_min_unit_price,
    AVG(unit_price) OVER w_ordered last4_avg_unit_price,
    MAX(unit_price) OVER w_ordered last4_max_unit_price,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) event_seq
FROM order_items
WHERE quantity >= 4
WINDOW w_ordered AS (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
