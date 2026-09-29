WITH scored AS (
    SELECT
        *,
        COUNT(revenue) OVER w_recent AS last7_count_revenue,
        SUM(revenue) OVER w_recent AS last7_sum_revenue,
        AVG(revenue) OVER w_recent AS last7_avg_revenue
    FROM store_sales
    WHERE sale_date < '2025-09-01'
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        last7_count_revenue,
        last7_sum_revenue,
        last7_avg_revenue,
        DATE_SUB(sale_date, 59) AS sale_date_minus_59d
    FROM scored
    WHERE sale_date >= DATE '2023-03-02'
),
cleaned_store_sales AS (
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        last7_count_revenue,
        CAST(units AS DOUBLE) AS units_num
    FROM cleaned
    WHERE sale_date >= DATE '2023-09-11'
),
with_dims AS (
    SELECT
        e.*,
        b.city,
        b.region
    FROM cleaned_store_sales e
    LEFT JOIN stores b
        ON e.store_id = b.id
),
kept_keys AS (
    SELECT store_id
    FROM with_dims
    INTERSECT
    SELECT store_id
    FROM store_sales
    WHERE sale_date >= '2025-04-28'
),
verified AS (
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.store_id = k.store_id
)
SELECT
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    ROUND(SUM(revenue) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS revenue_per_chiang_mai,
    APPROX_PERCENTILE(last7_count_revenue, ARRAY(0.25, 0.5, 0.75)) AS last7_count_revenue_quartiles,
    MAX(units_num) FILTER (WHERE city = 'Tokyo') AS tokyo_max_units_num,
    SUM(CASE WHEN city = 'London' THEN revenue ELSE 0 END) AS london_revenue
FROM verified
GROUP BY DATE_TRUNC('MONTH', sale_date)
