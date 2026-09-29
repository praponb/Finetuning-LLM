WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        opened_date,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
        TRIM(store_name) AS clean_store_name,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
      AND opened_date > TO_DATE('2023-05-30')
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.store_sale_revenue,
        latest.store_sale_sale_date
    FROM deduped s
    JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.product_id = 7
        )
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.days_since_opened_date,
        s.clean_store_name,
        s.dedup_rank,
        s.store_sale_revenue,
        s.store_sale_sale_date,
        latest.inventory_quantity,
        latest.inventory_last_restock
    FROM kept s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN inventory_quantity >= 243 THEN 'LARGE' ELSE 'SMALL' END AS inventory_quantity_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    PERCENTILE_APPROX(inventory_quantity, 0.5) AS inventory_quantity_p50,
    COUNT(*) AS event_count,
    ROUND(AVG(store_sale_revenue), 2) AS avg_store_sale_revenue
FROM latest_match
GROUP BY CASE WHEN inventory_quantity >= 243 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY event_count;
