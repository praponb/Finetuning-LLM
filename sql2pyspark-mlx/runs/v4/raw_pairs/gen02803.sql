WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CAST(quantity AS STRING) AS quantity_num
    FROM inventory
    WHERE last_restock >= DATE '2023-06-16'
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    product_id,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    SUM(quantity) AS total_quantity
FROM base
GROUP BY DATE_TRUNC('YEAR', last_restock), product_id
ORDER BY quantity_stddev DESC;
