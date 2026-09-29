WITH valid AS (
    SELECT *, INITCAP(TRIM(city)) AS city_title
    FROM suppliers
    WHERE city IS NOT NULL
),
metrics AS (
    SELECT
        id,
        supplier_name,
        country,
        rating,
        MAX(rating) OVER (PARTITION BY city ORDER BY rating ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_rating,
        COUNT(rating) OVER (PARTITION BY city ORDER BY rating ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_rating
    FROM valid
),
not_flagged AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.supplier_id IS NOT NULL
        )
),
with_latest AS (
    SELECT
        s.*,
        x.product_category,
        x.product_launch_date
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.category AS product_category, c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    DATE_TRUNC('YEAR', product_launch_date) AS year_start,
    COUNT(CASE WHEN product_category = 'Grocery' THEN 1 END) AS grocery_count,
    APPROX_PERCENTILE(last4_count_rating, ARRAY(0.25, 0.5, 0.75)) AS last4_count_rating_quartiles,
    COUNT(*) FILTER (WHERE product_category = 'Fashion') AS fashion_rows
FROM with_latest
GROUP BY 1
HAVING COUNT(*) FILTER (WHERE product_category = 'Fashion') > 1
ORDER BY fashion_rows DESC
