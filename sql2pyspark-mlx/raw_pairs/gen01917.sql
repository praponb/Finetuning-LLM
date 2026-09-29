WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) = 1
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.region,
        s.opened_date,
        s.sqft,
        x.inventory_quantity
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        inventory_quantity,
        AVG(inventory_quantity) OVER (PARTITION BY MONTH(opened_date), city ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) inventory_quantity_avg_6,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_sqft,
        LAST_VALUE(region) IGNORE NULLS OVER (PARTITION BY region ORDER BY inventory_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_region,
        DENSE_RANK() OVER (ORDER BY sqft NULLS LAST) sqft_dense_rank
    FROM with_last
    WHERE sqft > 15411
),
deduped_stores AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY inventory_quantity, id) = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    CASE WHEN inventory_quantity >= 236 THEN 'HIGH' ELSE 'LOW' END AS inventory_quantity_tier,
    COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count,
    ROUND(SUM(sqft_dense_rank) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS sqft_dense_rank_per_tokyo,
    COUNT_IF(region = 'Central') AS central_events,
    GROUPING_ID() AS grouping_id
FROM deduped_stores
GROUP BY ROLLUP (DATE_TRUNC('WEEK', opened_date), CASE WHEN inventory_quantity >= 236 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY east_count NULLS FIRST
