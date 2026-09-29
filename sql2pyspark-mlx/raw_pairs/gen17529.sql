WITH filtered AS (
    SELECT
        *,
        CASE WHEN quantity >= 9 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END AS quantity_tier,
        CASE WHEN unit_price >= 310 THEN 'High' WHEN unit_price >= 100 THEN 'Medium' ELSE 'Low' END AS unit_price_band
    FROM order_items
    WHERE order_id IN (3, 6, 38)
)
SELECT
    id,
    quantity,
    quantity_tier,
    FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
    AVG(unit_price) OVER (ORDER BY unit_price ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS unit_price_avg_2,
    NTILE(3) OVER w_part AS unit_price_bucket
FROM filtered
WINDOW w_part AS (ORDER BY unit_price DESC, id ASC)
