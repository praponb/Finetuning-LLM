WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        TRIM(store_name) AS clean_store_name,
        COALESCE(region, 'N/A') AS region_filled
    FROM stores
    WHERE region IS NOT NULL
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.region,
        s.opened_date,
        s.sqft,
        s.clean_store_name,
        s.region_filled,
        top1.shipping_fee
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.inventory_quantity,
        top1.last_restock
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY last_restock NULLS LAST, id) AS dedup_rank
        FROM latest_match
    ) t
    WHERE dedup_rank = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.amount < 430
        )
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM eligible
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY dedup_rank DESC) = 1
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            city,
            region,
            sqft
        FROM ranked
    ) src
    PIVOT (
        SUM(sqft) AS amount,
        COUNT(1) AS n
        FOR region IN ('South', 'North')
    )
)
SELECT
    city,
    South_amount,
    North_amount,
    North_n,
    CASE WHEN city RLIKE '^(Tokyo|New York)$' THEN 'GROUP_B' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS city_zone
FROM wide
ORDER BY South_amount
