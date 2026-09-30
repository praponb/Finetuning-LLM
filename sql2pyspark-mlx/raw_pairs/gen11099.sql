WITH not_flagged AS (
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date < s.launch_date
        )
),
with_latest AS (
    SELECT
        s.id,
        s.category,
        s.brand,
        s.price,
        s.stock_qty,
        s.launch_date,
        s.tags,
        s.supplier_id,
        x.last_restock
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    SELECT
        id,
        category,
        price,
        stock_qty,
        launch_date,
        tags,
        last_restock,
        ROW_NUMBER() OVER (ORDER BY last_restock, id) AS seq_no,
        LAG(stock_qty, 2, 0) OVER w_part AS prev_stock_qty,
        AVG(stock_qty) OVER w_recent AS stock_qty_avg_2_prior
    FROM with_latest
    WINDOW w_part AS (ORDER BY stock_qty, id), w_recent AS (PARTITION BY supplier_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
latest AS (
    SELECT
        id,
        category,
        stock_qty,
        tags,
        last_restock,
        prev_stock_qty
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
kept_keys AS (
    SELECT id, category
    FROM latest
    EXCEPT
    SELECT id, category
    FROM products
    WHERE cost <= 270
),
verified AS (
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.id = k.id AND s.category = k.category
)
SELECT
    category,
    stock_qty,
    tags,
    prev_stock_qty,
    CASE WHEN category RLIKE '^(Sports|Grocery)$' THEN 'GROUP_B' WHEN category RLIKE '^(Electronics|Home)$' THEN 'CORE' ELSE 'GROUP_A' END AS category_zone
FROM verified
ORDER BY stock_qty NULLS FIRST;
