WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        ROUND(units * 1.15, 2) AS units_adjusted,
        CASE WHEN units >= 42 THEN 'High' WHEN units >= 13 THEN 'Medium' ELSE 'Low' END AS units_band
    FROM store_sales
    WHERE sale_date > DATE '2023-09-29'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(units_adjusted * 100, 2) AS units_adjusted_adjusted,
        units * units_adjusted AS units_times_units_adjusted
    FROM cleaned
    WHERE sale_date >= '2024-06-01'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units_adjusted_adjusted DESC, id) AS row_num
    FROM valid
    QUALIFY row_num = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(units_adjusted_adjusted, units) AS greatest_units_adjusted_adjusted_units
    FROM deduped
    WHERE sale_date >= DATE '2023-03-01'
)
-- Window metrics over several different partitions
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units_band,
    units_adjusted_adjusted,
    units_times_units_adjusted,
    row_num,
    LAG(revenue) OVER w_grp AS prev_revenue,
    LAG(units_adjusted_adjusted) OVER w_grp AS prev_units_adjusted_adjusted,
    LEAD(units_adjusted_adjusted) OVER w_grp AS next_units_adjusted_adjusted,
    ROW_NUMBER() OVER w_grp AS row_seq,
    FIRST_VALUE(units, TRUE) OVER (PARTITION BY store_id ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_units
FROM filtered
WHERE units_adjusted_adjusted BETWEEN 31 AND 54
WINDOW w_grp AS (PARTITION BY product_id, store_id ORDER BY row_num, id);
