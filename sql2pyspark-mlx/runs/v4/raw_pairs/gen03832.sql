WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CEIL(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock >= TO_DATE('2024-01-19')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST, id ASC) = 1
),
enriched AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.brand,
        p.product_name
    FROM latest src
    LEFT JOIN products p
        ON src.product_id = p.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS latest_rank
        FROM enriched
    ) x
    WHERE latest_rank = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        d.category,
        d.price,
        d.launch_date
    FROM ranked a
    LEFT JOIN products d
        ON a.product_id = d.id
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.last_restock,
    s.brand,
    s.product_name,
    s.latest_rank,
    s.category,
    s.price,
    CASE WHEN brand RLIKE '^(Acme|Initech)$' THEN 'CORE' WHEN brand RLIKE '^(Globex|Umbrella)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS brand_zone,
    (SELECT MIN(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS min_inventory_quantity
FROM with_dims s
ORDER BY product_id DESC
