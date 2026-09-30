WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(quantity) AS quantity_whole,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE quantity BETWEEN 115 AND 203
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(quantity_whole) AS quantity_whole_whole
    FROM valid
    WHERE last_restock > DATE '2023-05-04'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
deduped_inventory AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC NULLS LAST, id ASC) AS latest_rank
    FROM deduped
    QUALIFY latest_rank = 1
),
report AS (
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.quantity,
        s.quantity_whole,
        s.quantity_whole_whole,
        s.latest_rank,
        (SELECT SUM(o2.discount) FROM orders o2 WHERE o2.store_id = s.store_id) AS sum_order_discount
    FROM deduped_inventory s
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    quantity_whole_whole,
    latest_rank,
    sum_order_discount
FROM report
