WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE last_restock >= DATE '2023-08-17'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        quantity_filled,
        SUM(quantity) OVER (PARTITION BY MONTH(last_restock), store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        LEAD(quantity_filled, 2) OVER (PARTITION BY store_id ORDER BY quantity, id) AS next_quantity_filled
    FROM valid
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC NULLS LAST, id) AS row_num
    FROM scored
    QUALIFY row_num = 1
),
ranked_inventory AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY running_quantity, id) AS rank_in_group
    FROM ranked
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Attach products attributes
    SELECT
        a.*,
        b.brand,
        b.price,
        b.product_name
    FROM ranked_inventory a
    LEFT JOIN products b
        ON a.product_id = b.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    product_id,
    measure,
    metric_value
FROM (
    SELECT
        id,
        product_id,
        next_quantity_filled,
        quantity_filled,
        price
    FROM enriched
) src
UNPIVOT (
    metric_value FOR measure IN (next_quantity_filled AS NEXT_QUANTITY_FILLED, quantity_filled AS QUANTITY_FILLED, price)
)
