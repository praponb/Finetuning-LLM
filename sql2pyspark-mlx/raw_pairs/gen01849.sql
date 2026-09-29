WITH cleaned AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        quantity - unit_price AS line_total,
        unit_price * quantity AS line_total_2
    FROM order_items
    WHERE unit_price BETWEEN 250 AND 340
)
SELECT
    CASE WHEN line_total >= 81 THEN 'HIGH' ELSE 'LOW' END AS line_total_tier,
    BIT_OR(quantity) AS quantity_bit_or,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(quantity, 0.5) AS quantity_p50,
    APPROX_PERCENTILE(line_total_2, 0.9) AS line_total_2_p90,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM cleaned
GROUP BY CASE WHEN line_total >= 81 THEN 'HIGH' ELSE 'LOW' END
ORDER BY line_total_2_p90 DESC
