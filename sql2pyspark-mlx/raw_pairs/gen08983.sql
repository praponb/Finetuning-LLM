WITH filtered AS (
    SELECT
        *,
        REGEXP_REPLACE(store_name, ' ', '') AS store_name_slug,
        COALESCE(region, 'NONE') AS region_filled
    FROM stores
    WHERE city = 'New York'
),
with_last AS (
    SELECT
        s.id,
        s.city,
        s.region,
        s.opened_date,
        top1.store_sale_sale_date,
        top1.store_sale_units
    FROM filtered s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date >= s.opened_date
        )
),
with_last_stores AS (
    SELECT
        s.id,
        s.region,
        s.store_sale_units,
        x.status,
        x.order_date
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.status, c.order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    region,
    store_sale_units,
    status,
    CASE WHEN region RLIKE '^(South|Central)$' THEN 'EXTENDED' WHEN region RLIKE '^(North|East)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS region_zone
FROM with_last_stores
