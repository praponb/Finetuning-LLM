WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        SUM(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        LAST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY quantity) AS quantity_cume_dist
    FROM order_items
)
-- Multi-level aggregation
SELECT
    product_id,
    PERCENTILE_APPROX(cumulative_quantity, 0.9) AS cumulative_quantity_p90,
    AVG(quantity_cume_dist) AS avg_quantity_cume_dist,
    BIT_AND(quantity) AS quantity_bit_and
FROM scored
GROUP BY product_id
HAVING PERCENTILE_APPROX(cumulative_quantity, 0.9) < 1000000
ORDER BY quantity_bit_and DESC;
