SELECT
    CASE WHEN quantity >= 195 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    APPROX_PERCENTILE(quantity, 0.95) AS quantity_p95,
    BIT_OR(quantity) AS quantity_bit_or,
    BIT_AND(quantity) AS quantity_bit_and,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    COUNT(DISTINCT id) AS distinct_ids
FROM inventory
GROUP BY CASE WHEN quantity >= 195 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY distinct_ids NULLS FIRST;
