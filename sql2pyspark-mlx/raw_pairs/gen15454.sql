WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT product_id
    FROM order_items
    INTERSECT ALL
    SELECT product_id
    FROM order_items
    WHERE unit_price BETWEEN 160 AND 380
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    CORR(quantity, unit_price) AS corr_quantity_unit_price,
    PERCENTILE_APPROX(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    BIT_AND(quantity) AS quantity_bit_and,
    STDDEV_SAMP(unit_price) AS unit_price_stddev
FROM verified
GROUP BY product_id
ORDER BY unit_price_stddev DESC NULLS FIRST
