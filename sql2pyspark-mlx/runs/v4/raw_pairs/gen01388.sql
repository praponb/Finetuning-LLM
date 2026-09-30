WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(country) AS country_lower
    FROM suppliers
    WHERE city IS NOT NULL
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.product_brand,
        latest.category
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.brand AS product_brand, c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date BETWEEN '2023-09-01' AND '2023-11-09'
        )
),
not_flagged_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IN (4, 22, 33)
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    country,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
    STDDEV_SAMP(rating) AS rating_stddev,
    MIN(rating) AS min_rating,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    COUNT(*) FILTER (WHERE city = 'Tokyo') AS tokyo_rows,
    COUNT(*) FILTER (WHERE category = 'Fashion') AS fashion_rows,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(*) FILTER (WHERE category = 'Fashion') DESC) AS fashion_rows_percent_rank
FROM not_flagged_suppliers
GROUP BY rating_tier, country;
