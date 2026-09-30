WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        city,
        rating,
        UPPER(city) AS city_upper
    FROM suppliers
    WHERE city IS NOT NULL
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        s.rating,
        s.city_upper,
        latest.cost,
        latest.launch_date
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.cost, c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city_upper,
        s.cost,
        s.launch_date,
        x.category,
        x.product_stock_qty
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.category, c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
remaining_keys AS (
    -- Set difference on keys
    SELECT country, id
    FROM with_last
    EXCEPT DISTINCT
    SELECT country, id
    FROM suppliers
    WHERE country = 'DE'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    JOIN remaining_keys k ON s.country = k.country AND s.id = k.id
),
summary AS (
    -- Grouped metrics
    SELECT
        category,
        COUNT(DISTINCT country) AS distinct_countries,
        COUNT_IF(country = 'UK') AS uk_events,
        COUNT(*) FILTER (WHERE country = 'DE') AS de_rows
    FROM verified
    GROUP BY category
)
SELECT
    category,
    distinct_countries,
    de_rows,
    CASE WHEN category RLIKE '^(Home|Books)$' THEN 'GROUP_A' WHEN category RLIKE '^(Electronics|Sports)$' THEN 'CORE' ELSE 'PRIORITY' END AS category_zone
FROM summary
ORDER BY de_rows NULLS LAST
