WITH kept AS (
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date < s.opened_date
        )
),
latest_match AS (
    SELECT
        s.id,
        s.city,
        top1.channel
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.channel
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
valid AS (
    SELECT *, CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END AS is_singapore
    FROM latest_match
    WHERE channel IS NOT NULL
),
latest_match_stores AS (
    SELECT
        s.*,
        top1.inventory_quantity,
        top1.inventory_last_restock
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM latest_match_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.units <= 21
        )
),
base AS (
    SELECT
        *,
        SUBSTRING(city, 1, 4) AS city_prefix,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM not_flagged
    WHERE channel IS NOT DISTINCT FROM 'partner'
),
with_windows AS (
    SELECT
        id,
        city,
        channel,
        is_singapore,
        city_prefix,
        MAX(inventory_quantity) OVER (PARTITION BY city ORDER BY is_singapore, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_inventory_quantity,
        SUM(inventory_quantity) OVER (PARTITION BY city ORDER BY is_singapore, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_inventory_quantity,
        COUNT(inventory_quantity) OVER (PARTITION BY city ORDER BY is_singapore, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_inventory_quantity
    FROM base
)
SELECT
    id,
    city,
    last4_max_inventory_quantity,
    last4_sum_inventory_quantity,
    last4_count_inventory_quantity
FROM with_windows
ORDER BY last4_max_inventory_quantity;
