WITH valid AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), opened_date) / 7 AS weeks_since_opened_date,
        COALESCE(region, 'N/A') AS region_filled
    FROM stores
    WHERE sqft BETWEEN 5328 AND 15701
),
enriched AS (
    SELECT
        id,
        region,
        opened_date,
        sqft,
        weeks_since_opened_date,
        region_filled,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        AVG(sqft) OVER (PARTITION BY city, region ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sqft_avg_5,
        NTILE(3) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS sqft_bucket
    FROM valid
    WHERE store_name IS NOT NULL
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY weeks_since_opened_date DESC NULLS FIRST, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
with_latest AS (
    SELECT
        s.id,
        s.opened_date,
        s.sqft,
        s.weeks_since_opened_date,
        s.region_filled,
        s.sqft_bucket,
        latest.inventory_last_restock
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    opened_date,
    sqft,
    weeks_since_opened_date,
    region_filled,
    inventory_last_restock
FROM with_latest
ORDER BY weeks_since_opened_date, sqft
