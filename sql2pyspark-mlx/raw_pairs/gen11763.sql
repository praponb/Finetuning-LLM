WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue * units AS revenue_per_unit,
        units - revenue AS revenue_per_unit_2
    FROM store_sales
    WHERE sale_date > DATE '2023-07-18'
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        store_id,
        revenue_per_unit,
        revenue_per_unit_2
    FROM filtered
) src
UNPIVOT (
    reading FOR metric_name IN (revenue_per_unit AS REVENUE_PER_UNIT_VAL, revenue_per_unit_2 AS REVENUE_PER_UNIT_2_VAL)
);
