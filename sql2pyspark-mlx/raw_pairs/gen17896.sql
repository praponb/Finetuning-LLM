WITH enriched AS (
    -- Attach products attributes
    SELECT src.*, d.launch_date
    FROM order_items src
    INNER JOIN products d
        ON src.product_id = d.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY launch_date DESC NULLS LAST) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(quantity) OVER w_part AS running_min_quantity,
        COUNT(quantity) OVER w_part AS running_count_quantity,
        MAX(quantity) OVER w_part AS running_max_quantity,
        AVG(quantity) OVER w_part AS running_avg_quantity,
        PERCENT_RANK() OVER (ORDER BY quantity DESC NULLS LAST) AS quantity_percent_rank
    FROM deduped
    WHERE unit_price >= 200
    WINDOW w_part AS (PARTITION BY order_id, product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        rank_in_group,
        running_min_quantity,
        running_count_quantity,
        running_max_quantity,
        quantity_percent_rank,
        DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date
    FROM scored
    WHERE product_id IS NOT NULL
      AND order_id IN (9, 26)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, product_id
    FROM valid
    INTERSECT ALL
    SELECT id, product_id
    FROM order_items
    WHERE order_id IN (3, 26)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.product_id = k.product_id
)
SELECT
    s.product_id,
    s.rank_in_group,
    s.running_min_quantity,
    s.running_count_quantity,
    s.running_max_quantity,
    s.quantity_percent_rank,
    (SELECT MAX(t2.quantity) FROM inventory t2 WHERE t2.product_id = s.product_id) AS max_inventory_quantity
FROM reconciled s
ORDER BY rank_in_group ASC;
