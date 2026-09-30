WITH enriched AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.product_name,
        p.launch_date,
        p.category
    FROM inventory a
    INNER JOIN products p
        ON a.product_id = p.id
    WHERE p.brand = 'Initech'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST, id) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.price,
        p.launch_date AS product_launch_date,
        p.brand
    FROM deduped e
    LEFT JOIN products p
        ON e.product_id = p.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_id,
        product_name,
        category,
        latest_rank,
        price,
        brand
    FROM with_dims
    QUALIFY RANK() OVER (PARTITION BY product_id, store_id ORDER BY last_restock) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN price >= 1080 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    category,
    MAX(price) AS max_price,
    ROUND(AVG(price), 2) AS avg_price,
    ROUND(AVG(price), 2) AS avg_price_2,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list
FROM latest
GROUP BY GROUPING SETS ((CASE WHEN price >= 1080 THEN 'PREMIUM' ELSE 'STANDARD' END, category), (CASE WHEN price >= 1080 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
HAVING ROUND(AVG(price), 2) >= 0
ORDER BY price_tier;
