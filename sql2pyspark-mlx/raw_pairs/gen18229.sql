WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(units, 0) AS units_filled,
        DATE_TRUNC('week', sale_date) AS sale_date_period
    FROM store_sales
    WHERE sale_date > DATE '2023-05-01'
)
-- Running and moving aggregates
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units,
    revenue,
    sale_date_period,
    LAST_VALUE(revenue) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('YEAR', sale_date), product_id ORDER BY units_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue,
    DENSE_RANK() OVER (PARTITION BY store_id ORDER BY units NULLS FIRST) AS units_dense_rank,
    AVG(units_filled) OVER (PARTITION BY YEAR(sale_date) ORDER BY CAST(sale_date_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS units_filled_avg_90d,
    SUM(revenue) OVER (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue
FROM cleaned
