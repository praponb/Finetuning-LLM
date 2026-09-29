WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        cost - price AS margin,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand
    FROM products
    WHERE price < 630
      AND supplier_id IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        launch_date,
        resolved_brand
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date NULLS LAST, id) = 1
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.quantity
    FROM deduped s
    JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY brand ORDER BY launch_date NULLS LAST) AS dedup_rank
        FROM with_latest
    ) ranked
    WHERE dedup_rank = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        product_name,
        price,
        launch_date,
        resolved_brand
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC NULLS LAST, id) AS rank_in_group
        FROM ranked
    ) ranked
    WHERE rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    CASE WHEN price >= 900 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS row_count
FROM latest
GROUP BY ROLLUP (DATE_TRUNC('YEAR', launch_date), DATE_TRUNC('QUARTER', launch_date), CASE WHEN price >= 900 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING distinct_ids >= 1
