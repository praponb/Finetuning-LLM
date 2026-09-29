WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(units / revenue, 2) AS revenue_per_unit
    FROM store_sales
    WHERE units >= 24
),
joined AS (
    -- Attach products attributes
    SELECT
        a.*,
        d.launch_date,
        d.category
    FROM base a
    LEFT JOIN products d
        ON a.product_id = d.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.store_name,
        b.opened_date
    FROM joined e
    LEFT JOIN stores b
        ON e.store_id = b.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        category,
        revenue,
        revenue_per_unit
    FROM enriched
) src
UNPIVOT (
    metric_value FOR metric IN (revenue AS REVENUE_VAL, revenue_per_unit)
)
