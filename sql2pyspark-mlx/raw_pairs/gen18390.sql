WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(region, 1, 2) AS region_prefix,
        COALESCE(region, 'NONE') AS region_clean
    FROM stores
    WHERE opened_date >= DATE '2023-06-10'
      AND region IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY sqft DESC) AS row_num
    FROM base
    QUALIFY row_num = 1
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.region,
        s.opened_date,
        s.sqft,
        s.region_prefix,
        s.region_clean,
        latest.inventory_last_restock,
        latest.quantity
    FROM deduped s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region,
        s.region_prefix,
        s.region_clean,
        s.inventory_last_restock,
        top1.discount
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.discount
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.inventory_last_restock
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN discount >= 0.37 THEN 'HIGH' ELSE 'LOW' END AS discount_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS event_count,
        COUNT(DISTINCT id) AS distinct_ids_2,
        COUNT(*) AS row_count,
        GROUPING(CASE WHEN discount >= 0.37 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
    FROM with_last
    GROUP BY ROLLUP (CASE WHEN discount >= 0.37 THEN 'HIGH' ELSE 'LOW' END)
    HAVING event_count < 1000
)
SELECT
    distinct_ids,
    event_count,
    grouping_level
FROM summary
ORDER BY grouping_level NULLS FIRST;
