WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(quantity AS DOUBLE) AS quantity_str,
        CASE WHEN unit_price >= 300 THEN 'High' WHEN unit_price >= 180 THEN 'Medium' ELSE 'Low' END AS unit_price_level
    FROM order_items
    WHERE order_id IN (19, 29, 40)
)
-- Running and moving aggregates
SELECT
    id,
    order_id,
    quantity_str,
    AVG(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_unit_price,
    MAX(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_unit_price,
    MIN(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_unit_price,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS row_seq
FROM valid;
