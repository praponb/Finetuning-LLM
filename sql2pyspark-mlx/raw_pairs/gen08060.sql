WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        region,
        sqft,
        DATEDIFF(CURRENT_DATE(), opened_date) days_since_opened_date,
        INITCAP(TRIM(region)) region_title
    FROM stores
    WHERE opened_date >= DATE '2023-08-02'
      AND region IS NOT NULL
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.store_sale_units
    FROM valid s
    JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY store_sale_units DESC, id) AS dedup_rank
        FROM latest_match
    ) x
    WHERE dedup_rank = 1
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.inventory_last_restock,
        latest.quantity
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    store_name,
    region,
    sqft,
    region_title,
    dedup_rank,
    inventory_last_restock,
    quantity
FROM with_last
ORDER BY inventory_last_restock NULLS LAST, sqft;
