SELECT
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    store_id,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    APPROX_PERCENTILE(quantity, 0.9) AS quantity_p90,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles_2
FROM inventory
GROUP BY quarter_start, store_id
ORDER BY quantity_p90 DESC;
