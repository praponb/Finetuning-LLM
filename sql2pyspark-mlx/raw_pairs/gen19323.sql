SELECT
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    store_id,
    product_id,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    APPROX_PERCENTILE(quantity, 0.5) AS quantity_p50,
    COUNT(*) AS cnt
FROM inventory
GROUP BY DATE_TRUNC('QUARTER', last_restock), store_id, product_id;
