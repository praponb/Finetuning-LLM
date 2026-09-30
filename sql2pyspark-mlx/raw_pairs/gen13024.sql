WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(sqft AS STRING) AS sqft_str,
        COALESCE(region, 'UNKNOWN') AS region_clean
    FROM stores
    WHERE region IS NOT NULL
      AND MONTH(opened_date) IN (1, 4, 8)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date NULLS FIRST, id) AS rank_in_group
        FROM valid
    ) x
    WHERE rank_in_group = 1
),
latest_stores AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY rank_in_group DESC, id) AS rank_in_group_2
    FROM latest
    QUALIFY rank_in_group_2 = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date < s.opened_date
        )
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.region,
        s.opened_date,
        s.sqft_str,
        s.rank_in_group,
        top1.inventory_quantity,
        top1.last_restock
    FROM kept s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.opened_date,
        s.sqft_str,
        s.rank_in_group,
        s.last_restock,
        latest.inventory_last_restock
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    opened_date,
    sqft_str,
    rank_in_group
FROM with_latest
ORDER BY opened_date, rank_in_group
