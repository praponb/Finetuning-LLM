WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        ROUND(price, 0) AS price_rounded
    FROM products
    WHERE price <= 620
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.product_name,
        src.brand,
        src.stock_qty,
        src.launch_date,
        src.tags,
        src.supplier_id,
        src.price_rounded,
        p.city,
        p.rating,
        p.supplier_name
    FROM valid src
    LEFT JOIN suppliers p
        ON src.supplier_id = p.id
    WHERE LOWER(TRIM(p.supplier_name)) = 'lion city imports'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        rating - price_rounded AS rating_minus_price_rounded,
        COALESCE(city, 'OTHER') AS city_clean
    FROM joined
    WHERE city IS NOT NULL
      AND city IS DISTINCT FROM 'Chiang Mai'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(price_rounded) OVER (PARTITION BY YEAR(launch_date)) AS launch_date_year_avg_price_rounded,
        LAST_VALUE(rating) IGNORE NULLS OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating,
        NTILE(3) OVER (PARTITION BY brand ORDER BY rating DESC, id) AS rating_bucket
    FROM base
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id, brand ORDER BY launch_date NULLS FIRST, id) AS dedup_rank
        FROM metrics
    ) x
    WHERE dedup_rank = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date <= s.launch_date
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        brand,
        rating,
        rating_minus_price_rounded,
        price_rounded,
        last_known_rating
    FROM kept
) src
UNPIVOT (
    value FOR metric_name IN (rating AS RATING, rating_minus_price_rounded, price_rounded AS PRICE_ROUNDED_VAL, last_known_rating AS LAST_KNOWN_RATING)
);
