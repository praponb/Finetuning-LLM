WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue + units AS revenue_per_unit
    FROM store_sales
    WHERE revenue <= 2700
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        units,
        revenue_per_unit,
        ROUND(revenue_per_unit * 1.1, 2) AS adjusted_revenue_per_unit
    FROM filtered
    WHERE sale_date > TO_DATE('2023-02-14')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(adjusted_revenue_per_unit AS INT) AS adjusted_revenue_per_unit_int
    FROM valid
    WHERE units <= 21
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY adjusted_revenue_per_unit DESC, id) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
)
-- Running and moving aggregates
SELECT
    id,
    product_id,
    units,
    revenue_per_unit,
    MAX(adjusted_revenue_per_unit) OVER w_part AS running_max_adjusted_revenue_per_unit,
    AVG(adjusted_revenue_per_unit) OVER w_part AS running_avg_adjusted_revenue_per_unit,
    COUNT(adjusted_revenue_per_unit) OVER w_part AS running_count_adjusted_revenue_per_unit,
    AVG(adjusted_revenue_per_unit) OVER (ORDER BY rank_in_group, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS adjusted_revenue_per_unit_avg_6
FROM deduped
WINDOW w_part AS (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
