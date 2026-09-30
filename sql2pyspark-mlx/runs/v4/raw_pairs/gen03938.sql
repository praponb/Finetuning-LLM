WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        units * revenue AS revenue_per_unit,
        DATE_TRUNC('YEAR', sale_date) AS sale_date_period
    FROM store_sales
    WHERE sale_date >= TO_DATE('2023-07-18')
      AND sale_date BETWEEN '2024-10-23' AND '2024-12-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(revenue_per_unit, TRUE) OVER (ORDER BY sale_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue_per_unit,
        SUM(revenue_per_unit) OVER (ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue_per_unit,
        LEAD(units, 1) OVER (PARTITION BY product_id ORDER BY sale_date_period ASC, id ASC) AS next_units
    FROM cleaned
),
with_windows_store_sales AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_per_unit,
        sale_date_period,
        last_known_revenue_per_unit,
        cumulative_revenue_per_unit,
        next_units,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY sale_date_period ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_avg_3_prior,
        LAST_VALUE(last_known_revenue_per_unit, TRUE) OVER (PARTITION BY store_id ORDER BY revenue_per_unit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_revenue_per_unit,
        AVG(revenue_per_unit) OVER (PARTITION BY store_id, product_id ORDER BY last_known_revenue_per_unit ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS revenue_per_unit_avg_3,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_known_revenue_per_unit DESC, id ASC) AS event_seq,
        AVG(cumulative_revenue_per_unit) OVER w_ordered AS cumulative_revenue_per_unit_avg_5_prior
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY sale_date_period ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.sale_date,
    s.units,
    s.revenue,
    s.revenue_per_unit,
    s.sale_date_period,
    s.cumulative_revenue_per_unit,
    (SELECT MIN(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS min_inventory_quantity
FROM with_windows_store_sales s
ORDER BY cumulative_revenue_per_unit DESC NULLS FIRST, sale_date_period ASC;
