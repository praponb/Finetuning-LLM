SELECT
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    DATE_TRUNC('WEEK', last_restock) AS week_start,
    BIT_AND(quantity) AS quantity_bit_and,
    APPROX_PERCENTILE(quantity, 0.95) AS quantity_p95,
    MIN(quantity) AS min_quantity,
    APPROX_PERCENTILE(quantity, 0.9) AS quantity_p90,
    BIT_AND(quantity) AS quantity_bit_and_2,
    MIN(quantity) AS min_quantity_2
FROM inventory
GROUP BY DATE_TRUNC('QUARTER', last_restock), DATE_TRUNC('WEEK', last_restock)
ORDER BY min_quantity_2;
