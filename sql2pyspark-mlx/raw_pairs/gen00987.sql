WITH deduped AS (
    SELECT
        id,
        brand,
        cost,
        stock_qty,
        tags,
        supplier_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price NULLS LAST, id) AS rn
        FROM products
    ) t
    WHERE rn = 1
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(cost, TRUE) OVER w_running AS last_known_cost,
        ROW_NUMBER() OVER (ORDER BY stock_qty, id) AS row_seq,
        LAG(cost, 2) OVER w_hist AS prev_cost
    FROM deduped
    WINDOW w_running AS (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY brand ORDER BY cost, id)
),
deduped_products AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand, supplier_id ORDER BY last_known_cost DESC NULLS LAST, id) = 1
),
valid AS (
    SELECT
        *,
        ROUND(stock_qty * 1.1, 2) AS stock_qty_scaled,
        CASE WHEN cost >= 720 THEN 'High' WHEN cost >= 270 THEN 'Medium' ELSE 'Low' END AS cost_level,
        COALESCE(brand, 'NONE') AS brand_filled
    FROM deduped_products
    WHERE brand = 'Stark'
),
eligible AS (
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id IN (4, 10, 26, 35)
        )
),
with_last AS (
    SELECT s.*, latest.inventory_quantity
    FROM eligible s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.product_id IN (1, 10)
        )
)
SELECT
    id,
    brand,
    cost,
    tags,
    supplier_id,
    row_seq,
    prev_cost,
    stock_qty_scaled,
    brand_filled
FROM not_flagged
ORDER BY row_seq DESC NULLS LAST, stock_qty_scaled
