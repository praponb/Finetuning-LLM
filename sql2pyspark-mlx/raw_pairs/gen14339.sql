WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(rating) AS rating_whole,
        COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        rating,
        rating_whole
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating ASC, id ASC) AS rn
        FROM valid
    ) x
    WHERE rn = 1
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.product_launch_date
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_last
    EXCEPT ALL
    SELECT id
    FROM suppliers
    WHERE country IN ('US', 'JP')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    JOIN kept_keys k ON s.id = k.id
)
SELECT
    rating,
    rating_whole,
    product_launch_date
FROM reconciled
ORDER BY rating_whole ASC, rating DESC;
