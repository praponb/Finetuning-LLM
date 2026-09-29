WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, quantity + unit_price AS line_total
    FROM order_items
    WHERE order_id IN (1, 14, 39)
)
-- Running and moving aggregates
SELECT
    id,
    unit_price,
    line_total,
    CORR(unit_price, quantity) OVER (PARTITION BY product_id) AS corr_unit_price_quantity,
    SUM(quantity) OVER (PARTITION BY order_id ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
    ROW_NUMBER() OVER w_hist AS seq_no
FROM cleaned
WINDOW w_hist AS (PARTITION BY order_id, product_id ORDER BY quantity, id)
