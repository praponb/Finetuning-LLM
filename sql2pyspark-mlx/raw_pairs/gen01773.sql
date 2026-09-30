WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN units >= 32 THEN 'High' WHEN units >= 6 THEN 'Medium' ELSE 'Low' END units_band,
        CEIL(units) units_whole
    FROM store_sales
    WHERE sale_date >= DATE '2023-09-01'
      AND sale_date < '2023-12-06'
)
-- Aggregate with conditional metrics
SELECT
    store_id,
    BIT_AND(units) AS units_bit_and,
    CORR(units, revenue) AS corr_units_revenue,
    BIT_OR(units) AS units_bit_or,
    APPROX_PERCENTILE(units_whole, 0.9) AS units_whole_p90
FROM base
GROUP BY store_id
HAVING BIT_OR(units) > 1
ORDER BY units_whole_p90 DESC NULLS FIRST
