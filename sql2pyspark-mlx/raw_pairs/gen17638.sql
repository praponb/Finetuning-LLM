WITH kept AS (
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.store_id IN (1, 4, 6)
        )
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft DESC, id) AS latest_rank
    FROM kept
    QUALIFY latest_rank = 1
),
with_last AS (
    SELECT
        s.id,
        s.region,
        s.sqft,
        s.latest_rank,
        latest.last_restock,
        latest.quantity
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    SELECT
        id,
        region,
        sqft,
        last_restock,
        quantity,
        MIN(quantity) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_quantity,
        COUNT(quantity) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_quantity,
        AVG(quantity) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_quantity
    FROM with_last
),
with_latest AS (
    SELECT
        s.id,
        s.region,
        s.sqft,
        s.last4_min_quantity,
        s.last4_count_quantity,
        latest.inventory_quantity,
        latest.inventory_last_restock
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped_stores AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region ORDER BY last4_min_quantity DESC) AS latest_rank
        FROM with_latest
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    id,
    region,
    last4_min_quantity,
    inventory_quantity,
    inventory_last_restock,
    latest_rank
FROM deduped_stores
