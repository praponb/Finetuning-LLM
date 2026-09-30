WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(quantity, 1, 0) OVER (ORDER BY last_restock, id) AS next_quantity,
        MAX(quantity) OVER (PARTITION BY store_id) AS store_id_max_quantity
    FROM inventory
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        next_quantity,
        store_id_max_quantity,
        DATE_TRUNC('YEAR', last_restock) AS last_restock_period
    FROM with_windows
    WHERE last_restock <= '2025-08-24'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id, store_id
    FROM base
    EXCEPT
    SELECT product_id, store_id
    FROM inventory
    WHERE quantity > 149
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.product_id = k.product_id AND s.store_id = k.store_id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, store_id_max_quantity * quantity AS store_id_max_quantity_times_quantity
    FROM kept_rows
    WHERE quantity <= 162
)
SELECT
    s.id,
    s.product_id,
    s.last_restock,
    s.next_quantity,
    s.store_id_max_quantity,
    s.last_restock_period,
    (SELECT MIN(t2.quantity) FROM inventory t2 WHERE t2.product_id = s.product_id) AS min_inventory_quantity
FROM cleaned s
ORDER BY min_inventory_quantity NULLS FIRST
