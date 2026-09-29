WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN units >= 37 THEN 'High' WHEN units >= 7 THEN 'Medium' ELSE 'Low' END AS units_tier,
        ROUND(revenue / units, 2) AS revenue_per_unit
    FROM store_sales
    WHERE store_id IN (1, 4, 5)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC NULLS LAST, id ASC) = 1
),
deduped_store_sales AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue_per_unit DESC NULLS LAST, id ASC) AS rn
    FROM deduped
    QUALIFY rn = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY rn ASC) AS latest_rank
    FROM deduped_store_sales
    QUALIFY latest_rank = 1
)
-- Join a dimension and aggregate
SELECT
    dim.city,
    COUNT(*) AS rows_total,
    ROUND(SUM(s.units), 2) AS total_units
FROM ranked s
INNER JOIN stores dim ON s.store_id = dim.id
GROUP BY ROLLUP (dim.city)
ORDER BY rows_total DESC NULLS LAST
