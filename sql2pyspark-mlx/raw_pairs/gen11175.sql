WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, units - revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-06-03')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        units,
        revenue,
        revenue_per_unit,
        SUM(units) OVER (ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY revenue_per_unit ASC, id ASC) AS revenue_per_unit_bucket,
        LEAD(revenue_per_unit, 1) OVER (PARTITION BY store_id ORDER BY units ASC, id ASC) AS next_revenue_per_unit,
        SUM(revenue_per_unit) OVER (PARTITION BY product_id, store_id) AS product_id_sum_revenue_per_unit
    FROM cleaned
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue_per_unit ASC NULLS FIRST, id ASC) AS latest_rank
        FROM scored
    ) t
    WHERE latest_rank = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(revenue, product_id_sum_revenue_per_unit) AS greatest_revenue_product_id_sum_revenue_per_unit
    FROM deduped
    WHERE units <= 19
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    MAX(revenue) AS max_revenue,
    COUNT(*) AS event_count
FROM valid
GROUP BY product_id
HAVING max_revenue < 1000000
ORDER BY event_count ASC
