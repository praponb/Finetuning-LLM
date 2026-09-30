WITH with_latest AS (
    SELECT
        s.*,
        latest.product_cost,
        latest.product_category
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.cost AS product_cost, c.category AS product_category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    SELECT
        id,
        supplier_name,
        country,
        rating,
        product_category,
        AVG(product_cost) OVER (PARTITION BY country) AS country_avg_product_cost,
        NTILE(10) OVER (PARTITION BY product_category ORDER BY product_cost DESC, id) AS product_cost_bucket
    FROM with_latest
),
with_last AS (
    SELECT
        s.id,
        s.country,
        s.rating,
        s.country_avg_product_cost,
        s.product_cost_bucket,
        top1.product_category_2
    FROM metrics s
    JOIN LATERAL (
            SELECT c.category AS product_category_2
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    SELECT
        id,
        country,
        product_cost_bucket,
        CUME_DIST() OVER (ORDER BY country_avg_product_cost DESC) AS country_avg_product_cost_cume_dist,
        NTILE(5) OVER (PARTITION BY country ORDER BY product_cost_bucket DESC, id) AS product_cost_bucket_bucket
    FROM with_last
    WHERE product_category_2 <> 'Home'
),
totals AS (
    SELECT
        country,
        CASE WHEN product_cost_bucket_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS product_cost_bucket_bucket_tier,
        CASE WHEN country_avg_product_cost_cume_dist >= 0.64 THEN 'HIGH' ELSE 'LOW' END AS country_avg_product_cost_cume_dist_tier,
        MIN(product_cost_bucket) AS min_product_cost_bucket,
        AVG(country_avg_product_cost_cume_dist) FILTER (WHERE country = 'US') AS us_avg_country_avg_product_cost_cume_dist
    FROM scored
    GROUP BY country, CASE WHEN product_cost_bucket_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN country_avg_product_cost_cume_dist >= 0.64 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    country,
    product_cost_bucket_bucket_tier,
    country_avg_product_cost_cume_dist_tier
FROM totals
