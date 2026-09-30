WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        units - revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-06-02')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(revenue) OVER w_ordered AS running_avg_revenue,
        SUM(revenue) OVER w_ordered AS running_sum_revenue,
        COUNT(revenue) OVER w_ordered AS running_count_revenue,
        FIRST_VALUE(revenue_per_unit, TRUE) OVER w_grp AS first_known_revenue_per_unit
    FROM valid
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY MONTH(sale_date), store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        GREATEST(units, running_sum_revenue) AS greatest_units_running_sum_revenue,
        ROUND(running_sum_revenue, 2) AS running_sum_revenue_rounded
    FROM with_windows
    WHERE sale_date >= TO_DATE('2023-04-18')
      AND running_sum_revenue < 14000
),
with_windows_store_sales AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(revenue_per_unit) OVER (PARTITION BY store_id ORDER BY running_avg_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue_per_unit,
        AVG(revenue_per_unit) OVER (ORDER BY first_known_revenue_per_unit, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_per_unit_avg_3_prior
    FROM base
    WHERE revenue_per_unit < 48
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('WEEK', sale_date) AS week_start,
        CASE WHEN first_known_revenue_per_unit >= 74 THEN 'HIGH' ELSE 'LOW' END AS first_known_revenue_per_unit_tier,
        DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
        APPROX_PERCENTILE(revenue_per_unit_avg_3_prior, ARRAY(0.25, 0.5, 0.75)) AS revenue_per_unit_avg_3_prior_quartiles,
        AVG(running_sum_revenue) AS avg_running_sum_revenue,
        COUNT(*) AS cnt,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', sale_date) ORDER BY COUNT(*) DESC) AS cnt_percent_rank
    FROM with_windows_store_sales
    GROUP BY DATE_TRUNC('WEEK', sale_date), CASE WHEN first_known_revenue_per_unit >= 74 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', sale_date)
)
SELECT
    week_start,
    first_known_revenue_per_unit_tier,
    quarter_start,
    revenue_per_unit_avg_3_prior_quartiles,
    cnt_percent_rank
FROM summary;
