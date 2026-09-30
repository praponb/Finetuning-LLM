WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        LAST_DAY(sale_date) AS sale_date_month_end,
        CASE WHEN units >= 41 THEN 'High' WHEN units >= 7 THEN 'Medium' ELSE 'Low' END AS units_tier
    FROM store_sales
    WHERE sale_date >= DATE '2023-08-30'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY sale_date_month_end, id) AS row_seq,
        LAST_VALUE(units, TRUE) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('week', sale_date_month_end), product_id ORDER BY units DESC, id) AS event_seq
    FROM cleaned
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.sale_date,
        s.units,
        s.revenue,
        s.row_seq,
        s.event_seq,
        d.price,
        d.cost,
        d.product_name
    FROM with_windows s
    LEFT JOIN products d
        ON s.product_id = d.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_dims
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY sale_date) = 1
),
cleaned_store_sales AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(revenue * 1.1, 2) AS revenue_adjusted
    FROM latest
    WHERE event_seq >= 6
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(units) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_units,
        MAX(units) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_units,
        COUNT(units) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_units,
        AVG(units) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_units,
        SUM(price) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price
    FROM cleaned_store_sales
)
SELECT
    id,
    units,
    revenue,
    event_seq,
    product_name,
    revenue_adjusted,
    running_sum_units,
    running_max_units,
    cumulative_price,
    CASE WHEN product_name RLIKE '^(Running Shoes|Yoga Mat)$' THEN 'EXTENDED' WHEN product_name RLIKE '^(Laptop Pro 14|wireless mouse)$' THEN 'GROUP_B' ELSE 'CORE' END AS product_name_zone
FROM metrics
ORDER BY running_sum_units DESC
