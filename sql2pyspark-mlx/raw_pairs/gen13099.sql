WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        FLOOR(rating) AS rating_whole,
        CEIL(rating) AS rating_whole_2,
        COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE city IS NOT DISTINCT FROM 'Berlin'
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.stock_qty
    FROM valid s
    JOIN LATERAL (
            SELECT c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(rating) OVER (PARTITION BY city) AS city_min_rating,
        LAST_VALUE(stock_qty) IGNORE NULLS OVER (PARTITION BY country ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stock_qty,
        RANK() OVER (ORDER BY rating_whole_2 DESC) AS rating_whole_2_rank
    FROM latest_match
    WHERE stock_qty >= 382
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.price
    FROM enriched s
    JOIN LATERAL (
            SELECT c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.price > 1010
        )
)
SELECT
    id,
    supplier_name,
    country,
    rating,
    rating_whole,
    rating_whole_2,
    city_filled,
    price
FROM eligible
ORDER BY country;
