WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        ADD_MONTHS(last_restock, 6) AS last_restock_plus_months
    FROM inventory
    WHERE last_restock >= DATE '2023-02-28'
)
-- Window metrics over several different partitions
SELECT
    id,
    store_id,
    product_id,
    MAX(quantity) OVER (PARTITION BY product_id, store_id) AS product_id_max_quantity,
    LEAD(quantity, 1) OVER (ORDER BY last_restock_plus_months, id) AS next_quantity,
    LEAD(quantity, 2) OVER (PARTITION BY YEAR(last_restock), store_id ORDER BY last_restock_plus_months, id) AS next_quantity_2
FROM cleaned;
