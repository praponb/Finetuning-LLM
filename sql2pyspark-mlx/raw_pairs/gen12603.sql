SELECT
    product_id,
    COUNT(*) AS num_rows,
    SUM(quantity) AS total_quantity,
    BIT_OR(quantity) AS quantity_bit_or,
    PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90
FROM inventory
GROUP BY product_id
ORDER BY quantity_bit_or DESC;
