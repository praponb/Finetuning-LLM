WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(city)) AS city_title,
        COALESCE(city, 'N/A') AS resolved_city
    FROM suppliers
    WHERE country IN ('SG', 'JP')
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        s.rating,
        s.resolved_city,
        top1.category,
        top1.cost
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.category, c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.product_brand
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_suppliers AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.launch_date
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date <= s.launch_date
        )
),
summary AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('MONTH', launch_date) AS activity_month,
        MAX(cost) FILTER (WHERE product_brand = 'Globex') AS globex_max_cost,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list
    FROM eligible
    GROUP BY activity_month
)
SELECT
    activity_month,
    globex_max_cost,
    country_list
FROM summary;
