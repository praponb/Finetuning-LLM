WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE product_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN unit_price_filled >= 79 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_filled_tier,
    product_id,
    CASE WHEN quantity >= 7 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
    COUNT(*) AS cnt,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    APPROX_PERCENTILE(unit_price_filled, ARRAY(0.25, 0.5, 0.75)) AS unit_price_filled_quartiles,
    ROUND(AVG(quantity), 2) AS avg_quantity_2,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN unit_price_filled >= 79 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(*) DESC) AS cnt_percent_rank
FROM cleaned
GROUP BY CASE WHEN unit_price_filled >= 79 THEN 'LARGE' ELSE 'SMALL' END, product_id, CASE WHEN quantity >= 7 THEN 'HIGH' ELSE 'LOW' END
HAVING ROUND(AVG(quantity), 2) > 0
