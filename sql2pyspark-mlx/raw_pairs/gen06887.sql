WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        product_id,
        units
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date NULLS LAST, id) AS row_num
        FROM store_sales
    ) x
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    store_id,
    CASE WHEN units >= 40 THEN 'HIGH' ELSE 'LOW' END AS units_tier,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    BIT_OR(units) AS units_bit_or,
    SUM(units) AS total_units,
    MAX(units) AS max_units,
    STDDEV_SAMP(units) AS units_stddev,
    GROUPING_ID() AS grouping_level
FROM deduped
GROUP BY ROLLUP (store_id, CASE WHEN units >= 40 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY units_tier DESC;
