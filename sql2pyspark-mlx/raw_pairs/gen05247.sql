WITH with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.price,
        s.tags,
        latest.order_item_quantity
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest_products AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.inventory_quantity,
        top1.last_restock
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest_products_2 AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.price,
        s.order_item_quantity,
        x.order_item_quantity_2
    FROM with_latest_products s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity_2
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, category
    FROM with_latest_products_2
    EXCEPT ALL
    SELECT id, category
    FROM products
    WHERE supplier_id IN (5, 18, 30, 37)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest_products_2 s
    INNER JOIN matched_keys k ON s.id = k.id AND s.category = k.category
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, category
    FROM verified
    INTERSECT ALL
    SELECT id, category
    FROM products
    WHERE SIZE(tags) > 2
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.category = k.category
)
-- Multi-level aggregation
SELECT
    category,
    CASE WHEN order_item_quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END AS order_item_quantity_tier,
    MAX(price) AS max_price,
    COUNT(DISTINCT category) AS distinct_categories
FROM kept_rows
GROUP BY category, CASE WHEN order_item_quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END
HAVING MAX(price) >= 0
ORDER BY distinct_categories DESC;
