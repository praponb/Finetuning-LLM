WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(brand, ' - ', product_name) AS brand_product_name
    FROM products
    WHERE supplier_id IS NOT NULL
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.inventory_quantity
    FROM base s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last_products AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.cost,
        s.launch_date,
        s.tags,
        s.brand_product_name,
        s.inventory_quantity,
        latest.order_item_quantity
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(cost, TRUE) OVER (ORDER BY inventory_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        CUME_DIST() OVER (ORDER BY order_item_quantity NULLS FIRST) AS order_item_quantity_cume_dist,
        LEAD(cost, 1, 0) OVER w_recent AS next_cost,
        ROW_NUMBER() OVER w_grp AS row_seq,
        SUM(inventory_quantity) OVER w_ordered AS cumulative_inventory_quantity
    FROM with_last_products
    WINDOW w_recent AS (ORDER BY launch_date, id), w_grp AS (PARTITION BY MONTH(launch_date) ORDER BY order_item_quantity, id), w_ordered AS (ORDER BY order_item_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Running and moving aggregates
SELECT
    id,
    launch_date,
    tags,
    brand_product_name,
    inventory_quantity,
    order_item_quantity,
    first_known_cost,
    order_item_quantity_cume_dist,
    row_seq,
    cumulative_inventory_quantity,
    AVG(next_cost) OVER (ORDER BY inventory_quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS next_cost_avg_2,
    NTILE(5) OVER (ORDER BY order_item_quantity DESC, id) AS order_item_quantity_bucket,
    LAG(first_known_cost, 1) OVER w_grp AS prev_first_known_cost,
    ROW_NUMBER() OVER (ORDER BY order_item_quantity_cume_dist, id) AS row_seq_2,
    ROW_NUMBER() OVER (PARTITION BY YEAR(launch_date) ORDER BY launch_date DESC, id) AS row_seq_3
FROM scored
WINDOW w_grp AS (PARTITION BY YEAR(launch_date) ORDER BY first_known_cost, id)
