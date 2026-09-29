WITH cleaned AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), opened_date) / 365.25 AS years_since_opened_date,
        SUBSTRING(city, 1, 4) AS city_prefix
    FROM stores
    WHERE region IS NULL
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY region, city ORDER BY sqft) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
with_latest AS (
    SELECT
        s.id,
        s.years_since_opened_date,
        s.city_prefix,
        top1.store_sale_sale_date,
        top1.store_sale_revenue
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock < s.store_sale_sale_date
        )
),
enriched AS (
    SELECT
        id,
        years_since_opened_date,
        store_sale_revenue,
        NTILE(5) OVER (ORDER BY store_sale_revenue, id) AS store_sale_revenue_bucket,
        LAST_VALUE(store_sale_revenue, TRUE) OVER w_recent AS last_known_store_sale_revenue
    FROM kept
    WHERE store_sale_sale_date >= '2023-12-01'
    WINDOW w_recent AS (ORDER BY store_sale_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN years_since_opened_date >= 72 THEN 'HIGH' ELSE 'LOW' END AS years_since_opened_date_tier,
    CASE WHEN last_known_store_sale_revenue >= 4200 THEN 'HIGH' ELSE 'LOW' END AS last_known_store_sale_revenue_tier,
    SUM(years_since_opened_date) AS total_years_since_opened_date,
    ROUND(AVG(years_since_opened_date), 2) AS avg_years_since_opened_date,
    PERCENTILE_APPROX(last_known_store_sale_revenue, ARRAY(0.25, 0.5, 0.75)) AS last_known_store_sale_revenue_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(years_since_opened_date), 2) AS avg_years_since_opened_date_2
FROM enriched
GROUP BY ROLLUP (CASE WHEN years_since_opened_date >= 72 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN last_known_store_sale_revenue >= 4200 THEN 'HIGH' ELSE 'LOW' END)
HAVING avg_years_since_opened_date_2 > 0
ORDER BY years_since_opened_date_tier DESC;
