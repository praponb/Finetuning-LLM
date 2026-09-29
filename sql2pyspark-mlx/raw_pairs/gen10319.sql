WITH with_dims AS (
    SELECT
        e.*,
        p.region,
        p.opened_date,
        p.store_name
    FROM store_sales e
    LEFT JOIN stores p
        ON e.store_id = p.id
),
kept_keys AS (
    SELECT store_id, product_id
    FROM with_dims
    INTERSECT ALL
    SELECT store_id, product_id
    FROM store_sales
    WHERE store_id = 1
),
kept_rows AS (
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
enriched AS (
    SELECT
        *,
        CORR(revenue, units) OVER (PARTITION BY YEAR(opened_date), region) AS corr_revenue_units,
        CORR(revenue, units) OVER w_hist AS corr_revenue_units_2,
        CORR(units, revenue) OVER w_hist AS corr_units_revenue,
        CORR(units, revenue) OVER w_hist AS corr_units_revenue_2
    FROM kept_rows
    WINDOW w_hist AS (PARTITION BY region)
),
base AS (
    SELECT
        id,
        store_id,
        product_id,
        revenue,
        region,
        opened_date,
        store_name,
        corr_revenue_units,
        corr_revenue_units_2,
        corr_units_revenue,
        corr_units_revenue_2,
        CASE WHEN units >= 45 THEN 'High' WHEN units >= 13 THEN 'Medium' ELSE 'Low' END AS units_tier,
        COALESCE(region, 'N/A') AS region_clean
    FROM enriched
    WHERE sale_date >= TO_DATE('2023-04-10')
      AND corr_revenue_units_2 < 0.6
),
joined AS (
    SELECT
        e.*,
        s.sqft,
        s.city,
        s.opened_date AS store_opened_date
    FROM base e
    LEFT JOIN stores s
        ON e.store_id = s.id
),
grouped AS (
    SELECT
        CASE WHEN corr_revenue_units >= 0.78 THEN 'HIGH' ELSE 'LOW' END AS corr_revenue_units_tier,
        DATE_TRUNC('WEEK', store_opened_date) AS week_start,
        ROUND(AVG(revenue), 2) AS avg_revenue,
        ROUND(SUM(corr_revenue_units_2) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS corr_revenue_units_2_per_chiang_mai,
        PERCENTILE_APPROX(revenue, 0.95) AS revenue_p95,
        COUNT(*) FILTER (WHERE region = 'East') AS east_rows,
        GROUPING(CASE WHEN corr_revenue_units >= 0.78 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
    FROM joined
    GROUP BY CUBE (CASE WHEN corr_revenue_units >= 0.78 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', store_opened_date))
)
SELECT
    week_start,
    revenue_p95,
    east_rows,
    grouping_id
FROM grouped
ORDER BY east_rows DESC NULLS LAST
