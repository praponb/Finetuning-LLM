WITH with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.quantity,
        x.last_restock
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(price / SUM(price) OVER (PARTITION BY MONTH(launch_date), category), 4) AS price_share,
        ROW_NUMBER() OVER (PARTITION BY category, brand ORDER BY quantity, id) AS row_seq,
        LAG(stock_qty, 2) OVER (PARTITION BY YEAR(last_restock), brand ORDER BY price, id) AS prev_stock_qty,
        NTILE(10) OVER (PARTITION BY category ORDER BY stock_qty DESC, id) AS stock_qty_bucket,
        PERCENT_RANK() OVER w_running AS quantity_percent_rank
    FROM with_last
    WHERE YEAR(launch_date) >= 2025
    WINDOW w_running AS (PARTITION BY brand ORDER BY quantity DESC)
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.inventory_quantity
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.last_restock
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY inventory_quantity DESC, id) AS rn
        FROM with_latest
    ) t
    WHERE rn = 1
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    CASE WHEN category RLIKE '^(Grocery|Books)$' THEN 'GROUP_A' WHEN category RLIKE '^(Electronics|Home)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS category_zone
FROM deduped
ORDER BY id DESC
