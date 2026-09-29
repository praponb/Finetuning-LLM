WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        rating,
        TRIM(country) clean_country,
        TRIM(city) clean_city
    FROM suppliers
    WHERE city IS NOT NULL
      AND city IS DISTINCT FROM 'Chiang Mai'
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.rating,
        s.clean_country,
        s.clean_city,
        latest.category,
        latest.product_brand
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.category, c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_last
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY rating DESC NULLS LAST, id) = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.supplier_id IS NOT NULL
        )
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.price,
        latest.product_category
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.price, c.category AS product_category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        clean_country,
        clean_city,
        category,
        product_brand,
        price,
        product_category,
        COALESCE(price, 0) price_filled
    FROM with_latest
    WHERE category IS NOT NULL
      AND product_category = 'Grocery'
)
-- Grouped metrics
SELECT
    CASE WHEN price >= 1180 THEN 'LARGE' ELSE 'SMALL' END AS price_tier,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM valid
GROUP BY CASE WHEN price >= 1180 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY num_cities DESC NULLS LAST
