WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM store_sales
    WHERE revenue >= 2400
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        days_since_sale_date,
        CEIL(days_since_sale_date) AS days_since_sale_date_whole
    FROM filtered
    WHERE revenue IS NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        days_since_sale_date,
        days_since_sale_date_whole,
        LAST_VALUE(days_since_sale_date, TRUE) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_sale_date,
        MAX(days_since_sale_date_whole) OVER (PARTITION BY product_id) AS product_id_max_days_since_sale_date_whole,
        LAG(units, 1) OVER (PARTITION BY product_id, store_id ORDER BY days_since_sale_date_whole, id) AS prev_units
    FROM valid
),
matched_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM scored
    EXCEPT DISTINCT
    SELECT product_id, id
    FROM store_sales
    WHERE product_id IN (2, 8, 10)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.product_id = k.product_id AND s.id = k.id
)
SELECT
    id,
    product_id,
    revenue,
    days_since_sale_date_whole,
    product_id_max_days_since_sale_date_whole,
    prev_units
FROM reconciled
ORDER BY prev_units DESC NULLS FIRST, product_id
