WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(store_name) AS store_name_lower
    FROM stores
    WHERE region IS NOT NULL
      AND region = 'North'
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.region,
        latest.inventory_last_restock,
        latest.inventory_quantity
    FROM base s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.inventory_last_restock,
        s.inventory_quantity,
        x.store_sale_sale_date,
        x.store_sale_revenue
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date <= s.inventory_last_restock
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.quantity >= 81
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        inventory_quantity + store_sale_revenue AS inventory_quantity_plus_store_sale_revenue,
        ROUND(store_sale_revenue * 100, 2) AS adjusted_store_sale_revenue
    FROM not_flagged
    WHERE store_sale_sale_date BETWEEN '2024-05-29' AND '2025-06-16'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date < s.store_sale_sale_date
        )
),
eligible_stores AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date < s.inventory_last_restock
        )
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', inventory_last_restock) AS activity_month,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(inventory_quantity) AS total_inventory_quantity,
    COUNT(DISTINCT id) AS distinct_ids_2,
    PERCENTILE_APPROX(inventory_quantity_plus_store_sale_revenue, 0.5) AS inventory_quantity_plus_store_sale_revenue_p50,
    COUNT(*) AS event_count
FROM eligible_stores
GROUP BY 1
ORDER BY distinct_ids
