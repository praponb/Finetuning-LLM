WITH base AS (
    SELECT
        *,
        units * revenue AS revenue_per_unit,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM store_sales
    WHERE sale_date > DATE '2023-05-04'
),
enriched AS (
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        revenue_per_unit,
        days_since_sale_date,
        SUM(days_since_sale_date) OVER w_running AS running_days_since_sale_date,
        AVG(units) OVER (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS units_avg_2,
        NTILE(10) OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS revenue_bucket
    FROM base
    WINDOW w_running AS (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    SELECT
        id,
        sale_date,
        revenue,
        days_since_sale_date,
        revenue_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue_per_unit DESC NULLS FIRST, id) AS rank_in_group
        FROM enriched
    ) ranked
    WHERE rank_in_group = 1
),
cleaned AS (
    SELECT
        id,
        days_since_sale_date,
        revenue_bucket,
        DATEDIFF(CURRENT_DATE(), sale_date) / 365.25 AS years_since_sale_date,
        LEAST(revenue_bucket, revenue) AS least_revenue_bucket_revenue
    FROM latest
    WHERE sale_date >= DATE '2023-08-27'
      AND sale_date > '2023-08-01'
)
SELECT
    id,
    revenue_bucket,
    least_revenue_bucket_revenue
FROM cleaned
ORDER BY revenue_bucket DESC NULLS FIRST
