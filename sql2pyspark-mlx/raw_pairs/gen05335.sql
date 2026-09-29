WITH enriched AS (
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        LAG(quantity, 1, 0) OVER w_recent AS prev_quantity,
        LAG(quantity, 1) OVER w_recent AS prev_quantity_2
    FROM inventory
    WINDOW w_recent AS (ORDER BY quantity, id)
)
SELECT
    product_id,
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    BIT_OR(quantity) AS quantity_bit_or,
    APPROX_PERCENTILE(prev_quantity, 0.95) AS prev_quantity_p95,
    APPROX_PERCENTILE(prev_quantity_2, ARRAY(0.25, 0.5, 0.75)) AS prev_quantity_2_quartiles
FROM enriched
GROUP BY product_id, DATE_TRUNC('QUARTER', last_restock)
ORDER BY avg_quantity;
