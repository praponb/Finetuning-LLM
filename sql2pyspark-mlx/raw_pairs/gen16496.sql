WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE last_restock > DATE '2023-08-29'
      AND quantity > 113
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.quantity,
        e.last_restock,
        b.launch_date,
        b.brand
    FROM cleaned e
    JOIN products b
        ON e.product_id = b.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY launch_date DESC, id) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(launch_date, 'yyyy-MM-dd') AS launch_date_label,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM ranked
    WHERE product_id = 4
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.last_restock,
    s.launch_date,
    s.launch_date_label,
    s.resolved_brand,
    (SELECT MAX(t2.quantity) FROM inventory t2 WHERE t2.store_id = s.store_id) AS max_inventory_quantity
FROM valid s
ORDER BY id NULLS LAST;
