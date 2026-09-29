WITH kept_keys AS (
    -- Set difference on keys
    SELECT store_id, product_id
    FROM inventory
    EXCEPT ALL
    SELECT store_id, product_id
    FROM inventory
    WHERE quantity > 158
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(quantity, 0) AS quantity_filled,
        FLOOR(quantity) AS quantity_whole
    FROM verified
    WHERE last_restock > DATE '2023-05-05'
      AND last_restock <= '2025-06-01'
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.store_id,
        s.quantity,
        s.last_restock,
        s.quantity_filled,
        s.quantity_whole,
        b.region,
        b.store_name,
        b.opened_date
    FROM filtered s
    LEFT JOIN stores b
        ON s.store_id = b.id
    WHERE b.opened_date > '2025-07-01'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, quantity - quantity_filled AS quantity_minus_quantity_filled
    FROM joined
    WHERE last_restock >= DATE '2023-06-04'
      AND region IN ('East', 'West', 'Central')
)
SELECT
    store_id,
    quantity,
    quantity_whole,
    region,
    store_name,
    quantity_minus_quantity_filled
FROM base
ORDER BY quantity_minus_quantity_filled DESC;
