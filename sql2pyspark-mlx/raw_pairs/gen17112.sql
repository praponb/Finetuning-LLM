WITH filtered AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        CASE WHEN brand IN ('Umbrella', 'Initech') THEN 'Core' ELSE 'Other' END AS brand_group,
        COALESCE(brand, 'OTHER') AS brand_clean
    FROM products
    WHERE launch_date >= DATE '2023-09-08'
      AND brand IS NOT NULL
),
with_dims AS (
    SELECT
        s.*,
        b.city,
        b.country
    FROM filtered s
    INNER JOIN suppliers b
        ON s.supplier_id = b.id
    WHERE b.country IN ('SG', 'DE')
),
kept AS (
    SELECT s.*
    FROM with_dims s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.product_id = 4
        )
),
enriched AS (
    SELECT
        id,
        brand,
        price,
        launch_date,
        tags,
        brand_group,
        brand_clean,
        city,
        AVG(cost) OVER (PARTITION BY MONTH(launch_date), city) AS launch_date_month_avg_cost,
        ROUND(price / SUM(price) OVER (PARTITION BY YEAR(launch_date), brand), 4) AS price_share,
        LAST_VALUE(city, TRUE) OVER (PARTITION BY DATE_TRUNC('month', launch_date), country ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city
    FROM kept
),
with_windows AS (
    SELECT
        id,
        brand,
        price,
        launch_date,
        tags,
        brand_group,
        brand_clean,
        price_share,
        NTILE(10) OVER (PARTITION BY brand ORDER BY price DESC, id) AS price_bucket,
        MIN(launch_date_month_avg_cost) OVER (PARTITION BY city) AS city_min_launch_date_month_avg_cost,
        NTILE(10) OVER (ORDER BY launch_date_month_avg_cost, id) AS launch_date_month_avg_cost_bucket
    FROM enriched
),
scored AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY brand ORDER BY price DESC, id) AS price_bucket_2,
        AVG(city_min_launch_date_month_avg_cost) OVER (ORDER BY price_share, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS city_min_launch_date_month_avg_cost_avg_2,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM with_windows
    WINDOW w_grp AS (ORDER BY launch_date_month_avg_cost_bucket DESC, id)
)
SELECT *
FROM (
    SELECT
        id,
        brand,
        price_share,
        price
    FROM scored
) src
UNPIVOT (
    val FOR kpi IN (price_share, price)
) long_rows
