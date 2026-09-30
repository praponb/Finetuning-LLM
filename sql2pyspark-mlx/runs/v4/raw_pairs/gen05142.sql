WITH filtered AS (
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        SUBSTRING(category, 1, 3) AS category_prefix,
        DATE_TRUNC('month', launch_date) AS launch_date_period
    FROM products
    WHERE supplier_id IS NOT NULL
      AND supplier_id IN (21, 23)
),
flagged AS (
    SELECT *, CASE WHEN launch_date_period - LAG(launch_date_period) OVER (PARTITION BY brand ORDER BY launch_date_period, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY brand ORDER BY launch_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_last AS (
    SELECT s.*, top1.inventory_last_restock
    FROM islands s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
matched_keys AS (
    SELECT id, brand
    FROM with_last
    EXCEPT DISTINCT
    SELECT id, brand
    FROM products
    WHERE brand = 'Umbrella'
),
reconciled AS (
    SELECT s.*
    FROM with_last s
    INNER JOIN matched_keys k ON s.id = k.id AND s.brand = k.brand
),
not_flagged AS (
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.quantity = 3
        )
)
SELECT
    s.product_name,
    s.brand,
    s.price,
    s.cost,
    s.launch_date,
    s.supplier_id,
    s.launch_date_period,
    s.is_new_burst,
    s.burst_id,
    (SELECT MAX(t2.cost) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS max_product_cost
FROM not_flagged s
ORDER BY supplier_id, launch_date_period;
