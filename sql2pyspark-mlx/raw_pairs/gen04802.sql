WITH filtered AS (
    SELECT *, SIZE(tags) AS tags_count
    FROM products
    WHERE launch_date > DATE '2023-09-20'
),
with_last AS (
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.price,
        s.tags_count,
        x.inventory_last_restock
    FROM filtered s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    SELECT
        s.id,
        s.price,
        top1.order_item_unit_price
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest_products AS (
    SELECT
        s.id,
        s.price,
        latest.inventory_last_restock,
        latest.quantity
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    SELECT
        *,
        LAG(price, 2) OVER (ORDER BY quantity, id) AS prev_price,
        CUME_DIST() OVER w_grp AS price_cume_dist
    FROM with_latest_products
    WINDOW w_grp AS (ORDER BY price DESC)
),
remaining_keys AS (
    SELECT id
    FROM enriched
    EXCEPT
    SELECT id
    FROM products
    WHERE cost <= 680
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.id = k.id
),
enriched_products AS (
    SELECT
        price,
        inventory_last_restock,
        prev_price,
        price_cume_dist,
        DENSE_RANK() OVER (ORDER BY price_cume_dist NULLS FIRST) AS price_cume_dist_dense_rank,
        DENSE_RANK() OVER (ORDER BY quantity DESC NULLS FIRST) AS quantity_dense_rank,
        CUME_DIST() OVER (ORDER BY price) AS price_cume_dist_2
    FROM reconciled
)
SELECT
    price,
    price_cume_dist_dense_rank,
    price_cume_dist_2
FROM enriched_products
ORDER BY price_cume_dist_dense_rank DESC NULLS FIRST
