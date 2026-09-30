WITH valid AS (
    SELECT
        *,
        FLOOR(rating) AS rating_whole,
        COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IS NOT NULL
        )
),
with_last AS (
    SELECT
        s.id,
        s.country,
        s.rating_whole,
        top1.product_category,
        top1.product_brand
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.category AS product_category, c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    SELECT
        id,
        country,
        LAG(rating_whole) OVER (PARTITION BY country, product_brand ORDER BY rating_whole, id) AS prev_rating_whole,
        ROW_NUMBER() OVER (PARTITION BY country, product_brand ORDER BY rating_whole, id) AS row_seq
    FROM with_last
),
ranked AS (
    SELECT
        id,
        country,
        prev_rating_whole
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY prev_rating_whole NULLS LAST, id) = 1
)
SELECT
    id,
    country,
    prev_rating_whole,
    CASE WHEN country RLIKE '^(US|UK)$' THEN 'OTHER_ZONE' WHEN country RLIKE '^(TH|SG)$' THEN 'PRIORITY' ELSE 'CORE' END AS country_zone
FROM ranked
ORDER BY id DESC NULLS FIRST;
