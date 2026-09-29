WITH valid AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CONCAT(supplier_name, '/', city) AS supplier_name_city,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM suppliers
    WHERE rating BETWEEN 3.3 AND 4.0
),
eligible AS (
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.cost > 660
        )
),
with_last AS (
    SELECT
        s.id,
        s.rating,
        s.city_filled,
        latest.product_brand,
        latest.category
    FROM eligible s
    JOIN LATERAL (
            SELECT c.brand AS product_brand, c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    SELECT
        id,
        city_filled,
        product_brand,
        category,
        NTILE(3) OVER (PARTITION BY product_brand ORDER BY rating ASC, id ASC) AS rating_bucket,
        LAST_VALUE(rating, TRUE) OVER (ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating
    FROM with_last
),
filtered AS (
    SELECT
        *,
        CONCAT(product_brand, ' ', category) AS product_brand_category,
        CASE WHEN category IN ('Home', 'Sports') THEN 'Group A' ELSE 'Other' END AS category_group
    FROM with_windows
    WHERE category IS NOT NULL
      AND category IS DISTINCT FROM 'Fashion'
),
kept AS (
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND ARRAY_CONTAINS(c.tags, 'bestseller')
        )
),
latest_match AS (
    SELECT
        s.id,
        s.category,
        s.category_group,
        x.stock_qty,
        x.product_cost
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty, c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    category,
    category_group,
    stock_qty
FROM latest_match
ORDER BY stock_qty DESC, id DESC
