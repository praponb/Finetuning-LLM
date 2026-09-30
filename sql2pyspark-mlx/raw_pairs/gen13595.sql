WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(quantity, 1) AS quantity_rounded,
        CASE WHEN quantity >= 250 THEN 'High' WHEN quantity >= 39 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM inventory
    WHERE product_id IN (3, 5, 6, 7)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_rounded DESC, id) = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM ranked
    INTERSECT
    SELECT id
    FROM inventory
    WHERE quantity >= 156
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN remaining_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        quantity_rounded,
        quantity_band,
        LAST_DAY(last_restock) AS last_restock_month_end,
        LEAST(quantity_rounded, quantity) AS least_quantity_rounded_quantity
    FROM kept_rows
    WHERE quantity_rounded BETWEEN 47 AND 75
),
joined AS (
    -- Attach stores attributes
    SELECT
        a.*,
        b.city,
        b.store_name
    FROM base a
    LEFT JOIN stores b
        ON a.store_id = b.id
)
SELECT
    s.store_id,
    s.product_id,
    s.quantity,
    s.last_restock_month_end,
    s.least_quantity_rounded_quantity,
    s.city,
    s.store_name,
    CASE WHEN city RLIKE '^(Tokyo|Bangkok)$' THEN 'EXTENDED' WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'GROUP_B' ELSE 'CORE' END AS city_zone,
    (SELECT MAX(t2.quantity) FROM inventory t2 WHERE t2.store_id = s.store_id) AS max_inventory_quantity
FROM joined s
ORDER BY last_restock_month_end DESC NULLS FIRST
