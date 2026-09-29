WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(quantity, 2) AS quantity_rounded,
        COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-05-17')
),
with_dims AS (
    -- Attach products attributes
    SELECT a.*, p.price
    FROM filtered a
    LEFT JOIN products p
        ON a.product_id = p.id
    WHERE p.cost < 280
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(price / SUM(price) OVER (PARTITION BY product_id), 4) AS price_share,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY quantity_filled DESC NULLS FIRST) AS quantity_filled_percent_rank,
        LAST_VALUE(quantity_filled, TRUE) OVER (PARTITION BY product_id, store_id ORDER BY quantity_rounded ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_filled
    FROM with_dims
),
with_windows_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity_filled,
        price,
        price_share,
        quantity_filled_percent_rank,
        last_known_quantity_filled,
        LAG(quantity_filled) OVER (PARTITION BY store_id, product_id ORDER BY quantity_filled_percent_rank ASC, id ASC) AS prev_quantity_filled,
        LEAD(quantity_filled) OVER (PARTITION BY store_id, product_id ORDER BY quantity_filled_percent_rank ASC, id ASC) AS next_quantity_filled,
        LEAD(last_known_quantity_filled) OVER (PARTITION BY store_id, product_id ORDER BY quantity_filled_percent_rank ASC, id ASC) AS next_last_known_quantity_filled,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity_filled_percent_rank ASC, id ASC) AS row_seq
    FROM with_windows
)
SELECT
    id,
    store_id,
    product_id,
    price,
    price_share,
    last_known_quantity_filled,
    prev_quantity_filled,
    next_quantity_filled,
    next_last_known_quantity_filled
FROM with_windows_inventory;
