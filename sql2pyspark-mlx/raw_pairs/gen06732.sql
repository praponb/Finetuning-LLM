WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) AS row_seq_2,
        NTILE(10) OVER (ORDER BY unit_price, id) AS unit_price_bucket,
        FIRST_VALUE(unit_price, TRUE) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price
    FROM order_items
    WHERE quantity BETWEEN 6 AND 8
)
-- Multi-level aggregation
SELECT
    order_id,
    CASE WHEN unit_price_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS unit_price_bucket_tier,
    BIT_OR(quantity) AS quantity_bit_or,
    MIN(unit_price_bucket) AS min_unit_price_bucket,
    ROUND(AVG(first_known_unit_price), 2) AS avg_first_known_unit_price,
    GROUPING(order_id) AS grouping_level
FROM enriched
GROUP BY ROLLUP (order_id, CASE WHEN unit_price_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END)
