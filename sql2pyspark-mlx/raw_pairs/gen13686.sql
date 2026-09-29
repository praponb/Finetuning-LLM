WITH valid AS (
    SELECT
        *,
        revenue * units AS revenue_per_unit,
        LEAST(revenue, units) AS least_revenue_units
    FROM store_sales
    WHERE sale_date >= DATE '2023-04-03'
)
SELECT
    id,
    product_id,
    units,
    revenue,
    revenue_per_unit,
    least_revenue_units,
    DENSE_RANK() OVER (PARTITION BY product_id ORDER BY revenue_per_unit DESC NULLS FIRST) AS revenue_per_unit_dense_rank,
    AVG(revenue_per_unit) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_per_unit_avg_6,
    LEAD(least_revenue_units, 1) OVER (ORDER BY sale_date, id) AS next_least_revenue_units,
    LAST_VALUE(least_revenue_units) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY least_revenue_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_least_revenue_units,
    CORR(units, revenue) OVER (PARTITION BY product_id) AS corr_units_revenue
FROM valid
