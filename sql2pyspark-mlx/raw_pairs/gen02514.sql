WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.quantity < 84
        )
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.price,
        s.tags,
        s.supplier_id,
        latest.order_item_quantity
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY supplier_id, category ORDER BY order_item_quantity NULLS LAST) AS dedup_rank
        FROM latest_match
    ) x
    WHERE dedup_rank = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.product_id IN (12, 24, 40)
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(price) OVER w_recent AS price_avg_6,
        CUME_DIST() OVER w_recent_2 AS price_cume_dist,
        AVG(price) OVER w_grp AS price_avg_3
    FROM not_flagged
    WINDOW w_recent AS (PARTITION BY supplier_id, category ORDER BY order_item_quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_recent_2 AS (PARTITION BY category ORDER BY price DESC NULLS FIRST), w_grp AS (PARTITION BY category ORDER BY dedup_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
latest_match_products AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.inventory_quantity,
        top1.inventory_last_restock
    FROM metrics s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        supplier_id,
        category,
        price_cume_dist
    FROM latest_match_products
) src
PIVOT (
    SUM(price_cume_dist) AS total,
    COUNT(1) AS item_count
    FOR category IN ('Books' AS books, 'Sports' AS sports)
)
