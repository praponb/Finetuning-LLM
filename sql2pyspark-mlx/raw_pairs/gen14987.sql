WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        SORT_ARRAY(tags) AS sorted_tags,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM products
    WHERE launch_date > TO_DATE('2023-09-26')
      AND launch_date BETWEEN '2023-03-08' AND '2024-03-01'
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.price,
        s.sorted_tags,
        top1.quantity
    FROM filtered s
    JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id IN (15, 24, 28)
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY quantity NULLS LAST, id) AS latest_rank
    FROM kept
    QUALIFY latest_rank = 1
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.inventory_quantity,
        x.inventory_last_restock
    FROM deduped s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    product_name,
    price,
    quantity,
    latest_rank,
    inventory_last_restock
FROM with_latest
ORDER BY price
