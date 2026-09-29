WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock >= DATE '2024-03-26'
      AND product_id IN (5, 7, 8, 9)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(quantity_whole) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_whole,
        LAST_VALUE(quantity, TRUE) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        LAG(quantity, 2, 0) OVER (PARTITION BY store_id ORDER BY quantity, id) AS prev_quantity
    FROM filtered
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, product_id
    FROM enriched
    INTERSECT
    SELECT id, product_id
    FROM inventory
    WHERE store_id IN (1, 3, 4, 5)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id AND s.product_id = k.product_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
        BIT_OR(quantity) AS quantity_bit_or,
        PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
        PERCENTILE_APPROX(last_known_quantity, 0.9) AS last_known_quantity_p90,
        MAX(quantity_whole) AS max_quantity_whole,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(quantity_whole, ARRAY(0.25, 0.5, 0.75)) AS quantity_whole_quartiles
    FROM reconciled
    GROUP BY quarter_start
)
SELECT
    quarter_start,
    quantity_bit_or,
    quantity_quartiles,
    last_known_quantity_p90,
    max_quantity_whole,
    distinct_ids
FROM stats
ORDER BY quantity_bit_or DESC
