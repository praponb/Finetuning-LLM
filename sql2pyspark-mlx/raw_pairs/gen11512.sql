WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC NULLS LAST) AS dedup_rank
    FROM inventory
    QUALIFY dedup_rank = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.quantity,
        a.last_restock,
        a.dedup_rank,
        s.region
    FROM latest a
    INNER JOIN stores s
        ON a.store_id = s.id
    WHERE s.opened_date <= '2024-08-01'
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, store_id
    FROM joined
    EXCEPT DISTINCT
    SELECT id, store_id
    FROM inventory
    WHERE last_restock <= '2024-11-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.id = k.id AND s.store_id = k.store_id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        store_id,
        quantity,
        last_restock,
        dedup_rank,
        region,
        CUME_DIST() OVER (PARTITION BY MONTH(last_restock) ORDER BY quantity DESC) quantity_cume_dist
    FROM verified
),
joined_inventory AS (
    -- Attach stores attributes
    SELECT
        a.store_id,
        a.region,
        p.city,
        p.region AS store_region,
        p.opened_date
    FROM enriched a
    INNER JOIN stores p
        ON a.store_id = p.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', region, store_region) region_store_region_key,
        COALESCE(region, 'unassigned') region_filled
    FROM joined_inventory
    WHERE region IS NOT NULL
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
        COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count,
        COUNT(*) FILTER (WHERE region = 'West') AS west_rows,
        COUNT(DISTINCT city) AS distinct_cities,
        COUNT(CASE WHEN region = 'North' THEN 1 END) AS north_count,
        COUNT(CASE WHEN store_region = 'South' THEN 1 END) AS south_count
    FROM valid
    GROUP BY quarter_start
    HAVING COUNT(*) FILTER (WHERE region = 'West') > 1
)
SELECT
    quarter_start,
    west_rows,
    south_count
FROM totals
ORDER BY quarter_start DESC NULLS FIRST
