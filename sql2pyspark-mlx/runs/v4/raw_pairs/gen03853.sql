WITH kept AS (
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.product_name LIKE '%Mat%'
        )
),
remaining_keys AS (
    SELECT country, city
    FROM kept
    EXCEPT
    SELECT country, city
    FROM suppliers
    WHERE city = 'London'
),
kept_rows AS (
    SELECT s.*
    FROM kept s
    JOIN remaining_keys k ON s.country = k.country AND s.city = k.city
),
not_flagged AS (
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.stock_qty < 184
        )
),
valid AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END AS is_berlin,
        REGEXP_REPLACE(supplier_name, ' ', '_') AS supplier_name_slug
    FROM not_flagged
    WHERE city IS NOT NULL
      AND rating IS NOT NULL
),
enriched AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        is_berlin,
        supplier_name_slug,
        CUME_DIST() OVER w_ordered AS rating_cume_dist,
        RANK() OVER (PARTITION BY city ORDER BY is_berlin NULLS LAST) AS is_berlin_rank
    FROM valid
    WINDOW w_ordered AS (PARTITION BY city ORDER BY rating NULLS LAST)
),
not_flagged_suppliers AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.brand <> 'Stark'
        )
),
summary AS (
    SELECT
        city,
        CASE WHEN rating >= 4.0 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
        COUNT(*) AS cnt,
        SIZE(COLLECT_SET(city)) AS num_cities
    FROM not_flagged_suppliers
    GROUP BY city, rating_tier
)
SELECT
    city,
    rating_tier,
    cnt,
    num_cities
FROM summary
ORDER BY cnt NULLS FIRST
