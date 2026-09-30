WITH with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.inventory_quantity,
        top1.last_restock
    FROM stores s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS dedup_rank
    FROM with_latest
    QUALIFY dedup_rank = 1
),
latest_stores AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY opened_date NULLS FIRST) AS latest_rank
    FROM latest
    QUALIFY latest_rank = 1
),
with_latest_stores AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.store_sale_sale_date
    FROM latest_stores s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date <= s.opened_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        region,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY dedup_rank DESC, id) AS row_seq,
        NTILE(3) OVER (PARTITION BY region, city ORDER BY inventory_quantity DESC, id) AS inventory_quantity_bucket,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share
    FROM with_latest_stores
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date >= s.opened_date
        )
),
totals AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('WEEK', opened_date) AS week_start,
        COUNT_IF(region = 'North') AS north_events,
        COUNT(CASE WHEN region = 'West' THEN 1 END) AS west_count,
        ROUND(SUM(inventory_quantity_bucket) / NULLIF(COUNT(CASE WHEN region = 'East' THEN 1 END), 0), 2) AS inventory_quantity_bucket_per_east,
        COUNT(DISTINCT id) AS distinct_ids,
        SORT_ARRAY(COLLECT_SET(region)) AS region_list,
        ROUND(SUM(inventory_quantity_bucket) / NULLIF(COUNT(CASE WHEN region = 'West' THEN 1 END), 0), 2) AS inventory_quantity_bucket_per_west
    FROM kept
    GROUP BY DATE_TRUNC('WEEK', opened_date)
    HAVING COUNT_IF(region = 'North') < 1000
)
SELECT
    week_start,
    north_events,
    west_count,
    inventory_quantity_bucket_per_east,
    distinct_ids,
    region_list,
    inventory_quantity_bucket_per_west
FROM totals
ORDER BY inventory_quantity_bucket_per_east NULLS LAST;
