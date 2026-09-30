WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        cost + stock_qty AS cost_plus_stock_qty,
        COALESCE(brand, 'N/A') AS brand_filled
    FROM products
    WHERE launch_date >= DATE '2023-09-12'
      AND brand IS NOT NULL
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.product_name,
        src.brand,
        src.price,
        src.stock_qty,
        src.launch_date,
        src.tags,
        src.supplier_id,
        src.cost_plus_stock_qty,
        src.brand_filled,
        p.rating
    FROM valid src
    LEFT JOIN suppliers p
        ON src.supplier_id <=> p.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN price >= 1010 THEN 'High' WHEN price >= 390 THEN 'Medium' ELSE 'Low' END AS price_tier
    FROM with_dims
    WHERE brand IS DISTINCT FROM 'Globex'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY supplier_id, brand), 4) AS rating_share,
        LAG(price, 1, 0) OVER (ORDER BY price, id) AS prev_price
    FROM filtered
),
joined AS (
    -- Attach suppliers attributes
    SELECT src.*, s.rating AS supplier_rating
    FROM scored src
    LEFT JOIN suppliers s
        ON src.supplier_id = s.id
),
joined_products AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.price,
        a.launch_date,
        a.supplier_id,
        a.cost_plus_stock_qty,
        a.rating,
        a.rating_share,
        a.prev_price,
        b.city,
        b.supplier_name
    FROM joined a
    INNER JOIN suppliers b
        ON a.supplier_id = b.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM joined_products s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.product_id IN (3, 7, 9, 10)
        )
)
SELECT
    s.id,
    s.launch_date,
    s.supplier_id,
    s.cost_plus_stock_qty,
    s.rating,
    s.city,
    CASE WHEN city RLIKE '^(Tokyo|Singapore)$' THEN 'CORE' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS city_zone,
    (SELECT MIN(t2.cost) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS min_product_cost
FROM kept s
ORDER BY cost_plus_stock_qty DESC
