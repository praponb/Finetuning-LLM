WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity NULLS FIRST, id) AS row_num
    FROM order_items
    QUALIFY row_num = 1
)
-- Grouped metrics
SELECT
    product_id,
    CASE WHEN unit_price >= 390 THEN 'HIGH' ELSE 'LOW' END AS unit_price_tier,
    CORR(unit_price, quantity) AS corr_unit_price_quantity,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles
FROM latest
GROUP BY product_id, unit_price_tier
ORDER BY corr_unit_price_quantity DESC;
