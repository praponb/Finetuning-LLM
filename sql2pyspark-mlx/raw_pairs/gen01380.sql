WITH filtered AS (
    SELECT
        id,
        category,
        brand,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        CASE WHEN category = 'Grocery' THEN 1 ELSE 0 END AS is_grocery
    FROM products
    WHERE launch_date > DATE '2023-04-03'
      AND supplier_id IS NOT NULL
),
with_dims AS (
    SELECT e.*, b.city
    FROM filtered e
    LEFT JOIN suppliers b
        ON e.supplier_id = b.id
),
with_last AS (
    SELECT
        s.id,
        s.category,
        s.brand,
        s.stock_qty,
        s.tags,
        s.city,
        x.inventory_last_restock
    FROM with_dims s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY stock_qty ASC NULLS FIRST) AS stock_qty_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY stock_qty ASC NULLS FIRST) AS stock_qty_percent_rank,
        MAX(stock_qty) OVER (PARTITION BY YEAR(inventory_last_restock), brand) AS inventory_last_restock_year_max_stock_qty
    FROM with_last
),
with_windows AS (
    SELECT
        id,
        category,
        tags,
        city,
        inventory_last_restock,
        stock_qty_dense_rank,
        stock_qty_percent_rank,
        inventory_last_restock_year_max_stock_qty,
        RANK() OVER (PARTITION BY brand ORDER BY stock_qty ASC NULLS LAST) AS stock_qty_rank,
        NTILE(3) OVER (PARTITION BY brand, city ORDER BY stock_qty_percent_rank DESC, id ASC) AS stock_qty_percent_rank_bucket,
        NTILE(4) OVER (PARTITION BY city ORDER BY stock_qty ASC, id ASC) AS stock_qty_bucket
    FROM metrics
    WHERE stock_qty BETWEEN 202 AND 386
),
scored AS (
    SELECT
        id,
        category,
        city,
        inventory_last_restock,
        stock_qty_dense_rank,
        stock_qty_percent_rank,
        inventory_last_restock_year_max_stock_qty,
        stock_qty_bucket,
        MIN(stock_qty_rank) OVER (PARTITION BY city ORDER BY stock_qty_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_stock_qty_rank,
        SUM(stock_qty_rank) OVER (PARTITION BY city ORDER BY stock_qty_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_stock_qty_rank,
        COUNT(stock_qty_rank) OVER (PARTITION BY city ORDER BY stock_qty_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_stock_qty_rank
    FROM with_windows
),
kept_keys AS (
    SELECT category, id
    FROM scored
    INTERSECT ALL
    SELECT category, id
    FROM products
    WHERE price BETWEEN 380 AND 690
),
kept_rows AS (
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.category = k.category AND s.id = k.id
)
SELECT
    id,
    city,
    inventory_last_restock,
    stock_qty_dense_rank,
    stock_qty_percent_rank,
    inventory_last_restock_year_max_stock_qty,
    running_min_stock_qty_rank,
    CASE WHEN city RLIKE '^(Singapore|Berlin)$' THEN 'OTHER_ZONE' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS city_zone
FROM kept_rows
ORDER BY running_min_stock_qty_rank DESC;
