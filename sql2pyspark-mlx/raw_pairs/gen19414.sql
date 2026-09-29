SELECT
    CASE WHEN unit_price >= 350 THEN 'HIGH' ELSE 'LOW' END AS unit_price_tier,
    product_id,
    BIT_OR(quantity) AS quantity_bit_or,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    ROUND(AVG(unit_price), 2) AS avg_unit_price_2,
    APPROX_PERCENTILE(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    BIT_AND(quantity) AS quantity_bit_and,
    COUNT(*) AS cnt,
    GROUPING_ID() AS gid
FROM order_items
GROUP BY CUBE (CASE WHEN unit_price >= 350 THEN 'HIGH' ELSE 'LOW' END, product_id)
HAVING quantity_bit_and >= 1;
