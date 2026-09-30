WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        REPLACE(store_name, ' ', '-') AS store_name_slug,
        CASE WHEN region = 'West' THEN 1 ELSE 0 END AS is_west
    FROM stores
    WHERE region IS NOT DISTINCT FROM 'Central'
      AND city IN ('Bangkok', 'Singapore')
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.region,
        s.opened_date,
        latest.inventory_last_restock,
        latest.inventory_quantity
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY inventory_last_restock DESC NULLS LAST, id) AS rn
        FROM latest_match
    ) ranked
    WHERE rn = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(inventory_quantity, 0) AS inventory_quantity_rounded,
        COALESCE(region, 'NONE') AS region_clean
    FROM ranked
    WHERE region IS NOT NULL
      AND region NOT IN ('West', 'East', 'Central')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_date,
        inventory_quantity,
        rn,
        LAG(inventory_quantity) OVER w_running AS prev_inventory_quantity,
        LAG(inventory_quantity_rounded) OVER w_running AS prev_inventory_quantity_rounded,
        ROW_NUMBER() OVER w_running AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY YEAR(inventory_last_restock), city ORDER BY inventory_quantity_rounded, id) AS event_seq_2
    FROM valid
    WINDOW w_running AS (PARTITION BY city, region ORDER BY rn, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(event_seq_2, 1) OVER (PARTITION BY DATE_TRUNC('week', opened_date) ORDER BY opened_date, id) AS prev_event_seq_2,
        NTILE(3) OVER (ORDER BY event_seq, id) AS event_seq_bucket
    FROM with_windows
),
latest_match_stores AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.opened_date,
        s.inventory_quantity,
        s.rn,
        s.prev_inventory_quantity_rounded,
        s.event_seq_bucket,
        latest.inventory_last_restock
    FROM enriched s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', inventory_last_restock) AS activity_month,
    CASE WHEN event_seq_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_bucket_tier,
    APPROX_PERCENTILE(prev_inventory_quantity_rounded, ARRAY(0.25, 0.5, 0.75)) AS prev_inventory_quantity_rounded_quartiles,
    ROUND(AVG(inventory_quantity), 2) AS avg_inventory_quantity,
    MAX(prev_inventory_quantity_rounded) AS max_prev_inventory_quantity_rounded,
    PERCENTILE_APPROX(inventory_quantity, 0.95) AS inventory_quantity_p95,
    COUNT(*) AS event_count,
    GROUPING(DATE_TRUNC('MONTH', inventory_last_restock)) AS grouping_level
FROM latest_match_stores
GROUP BY CUBE (DATE_TRUNC('MONTH', inventory_last_restock), CASE WHEN event_seq_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY max_prev_inventory_quantity_rounded DESC
